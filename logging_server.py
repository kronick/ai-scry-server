#coding=utf-8
import zmq
import time
import sys
import sqlite3
from datetime import datetime
# Prepare ZeroMQ context and sockets
zmq_context = zmq.Context()
zmq_socket = zmq_context.socket(zmq.REP)    
zmq_socket.bind("tcp://*:5500")

config = {
    "LOG_INTERVAL": 10, # Time in seconds to average requests/responses/timeouts/etc over
    "USER_TIMEOUT": 30, # If no requests are received from a user in this time, consider them dead
    "LOG_FILENAME": "log.db",
}

### Things to log:
# Number of workers online
# requests+responses/minute
# dropped requests/minute
# average wait time (need to keep track across multiple requests waiting due to full queue)
# requests+responses/minute for each user
# CPU usage per worker (average per minute)

stats = {
    "requests": 0,
    "replies": 0,
    "reply_time_sum": 0,
    "last_log_time": time.time(),
    "dropped": 0,
    "timeouts": 0,
    "max_reply_time": 0,
    "min_reply_time": sys.float_info.max,
    
    "active_workers_sum": 0,
    "ready_workers_sum": 0,
    "request_queue_length_sum": 0,
    "queue_samples": 0,
}

# Connect to the database and create tables if needed
with sqlite3.connect(config["LOG_FILENAME"]) as db:
    cursor = db.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS client_aggregate_stats(timestamp DATETIME DEFAULT CURRENT_TIMESTAMP PRIMARY KEY, active_users INTEGER, avg_wait REAL, min_wait REAL, max_wait REAL, request_rate REAL, reply_rate REAL, drop_rate REAL, timeout_rate REAL)")
    cursor.execute("CREATE TABLE IF NOT EXISTS user_stats(timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, uuid TEXT, request_rate REAL, PRIMARY KEY (timestamp, uuid))")
    cursor.execute("CREATE TABLE IF NOT EXISTS user_sessions(timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, uuid TEXT, request_rate REAL, duration REAL, request_count INTEGER, PRIMARY KEY (timestamp, uuid))")
    cursor.execute("CREATE TABLE IF NOT EXISTS worker_aggregate_stats(timestamp DATETIME DEFAULT CURRENT_TIMESTAMP PRIMARY KEY, active_workers REAL, ready_workers REAL, request_queue_length REAL)")
    
class User(object):
    def __init__(self, uuid):
        self.uuid = uuid
        self.request_timestamp = time.time()
        self.reply_timestamp = time.time()
        self.start_timestamp = time.time()
        
        self.requests = 0
        self.replies = 0
        self.last_log_time = time.time()
        self.waiting_for_reply = False
        
    def get_reply_time(self):
        return self.reply_timestamp - self.request_timestamp
        
    def request_made(self):           
        # Update the request timestamp but only if we're not already waiting for a response
        if not self.waiting_for_reply:
            self.waiting_for_reply = True
            self.request_timestamp = time.time()
            
        self.requests += 1
        
    def reply_received(self):
        self.reply_timestamp = time.time()
        if self.waiting_for_reply:
            timing = self.get_reply_time()
        else:
            timing = None
        self.waiting_for_reply = False
        
        return timing
        
    def log_request_rate(self):
        # Get average requests/second and reset requests counter
        dT = time.time() - self.last_log_time
        if dT > 0:
            rate = self.requests / dT
        else:
            rate = 0
        self.last_log_time = time.time()
        self.requests = 0
        return rate
        
    def end_session(self):
        request_count = self.requests
        request_rate = self.log_request_rate()
        duration = time.time() - self.start_timestamp - config["USER_TIMEOUT"]
        
        with sqlite3.connect(config["LOG_FILENAME"]) as db:
            cursor = db.cursor()
            cursor.execute("INSERT INTO user_sessions(uuid, request_rate, duration, request_count) VALUES(?, ?, ?, ?)", (self.uuid, request_rate, duration, request_count))
            db.commit()

    
users = {}

def cleanup_users():
    global users
    # Get rid of users that haven't made a request in a while
    survivors = {}
    for u in users:
        if time.time() - users[u].request_timestamp <= config["USER_TIMEOUT"]:
            survivors[u] = users[u]
        else:
            users[u].end_session()

    users = survivors


def log_stats():
    dT = time.time() - stats["last_log_time"]
    if dT < config["LOG_INTERVAL"]:
        return
    
    cleanup_users()
    
    avg_wait = 0
    if stats["replies"] > 0:
        avg_wait = stats["reply_time_sum"] / stats["replies"]

    requests_per_second = stats["requests"] / dT
    replies_per_second = stats["replies"] / dT
    dropped_per_second = stats["dropped"] / dT
    timeouts_per_second = stats["timeouts"] / dT       
    alive_users = users.keys()
    
    #print u"[{}] Stats from the last {:.2f} seconds:\n • {} active users\n • {:.2f} sec average wait time (range is {:.2f} - {:.2f} s)\n • {} requests/sec\n • {} replies/sec\n • {} dropped/sec\n • {} timeouts/sec".format(datetime.now(), dT, len(alive_users), avg_wait, stats["min_reply_time"], stats["max_reply_time"], requests_per_second, replies_per_second, dropped_per_second, timeouts_per_second)

    with sqlite3.connect(config["LOG_FILENAME"]) as db:
        cursor = db.cursor()
        cursor.execute("INSERT INTO client_aggregate_stats(active_users, avg_wait, min_wait, max_wait, request_rate, reply_rate, drop_rate, timeout_rate) VALUES(?, ?, ?, ?, ?, ?, ?, ?)", (len(alive_users), avg_wait, stats["min_reply_time"], stats["max_reply_time"], requests_per_second, replies_per_second, dropped_per_second, timeouts_per_second))
        db.commit()

    # Log averages of queue statistics as well
    log_worker_aggregate_stats(stats["active_workers_sum"] / float(stats["queue_samples"]),
                               stats["ready_workers_sum"] / float(stats["queue_samples"]),
                               stats["request_queue_length_sum"] / float(stats["queue_samples"]))
            
    # Reset the counters
    stats["requests"] = 0
    stats["replies"] = 0
    stats["reply_time_sum"] = 0
    stats["dropped"] = 0
    stats["timeouts"] = 0
    stats["last_log_time"] = time.time()
    stats["max_reply_time"] = 0
    stats["min_reply_time"] = sys.float_info.max
    stats["active_workers_sum"] = 0
    stats["ready_workers_sum"] = 0
    stats["request_queue_length_sum"] = 0
    stats["queue_samples"] = 0
    
def log_worker_aggregate_stats(active_workers, ready_workers, request_queue_length):
    with sqlite3.connect(config["LOG_FILENAME"]) as db:
        cursor = db.cursor()
        cursor.execute("INSERT INTO worker_aggregate_stats(active_workers, ready_workers, request_queue_length) VALUES(?, ?, ?)", (active_workers, ready_workers, request_queue_length))
        db.commit()


while True:
    frames = zmq_socket.recv_multipart()
    zmq_socket.send("OK")
    
    if len(frames) > 1:
        t = frames[0]
        # The following frame types should have a UUID associated with them. Retrieve or create new user record as needed        
        if t in ["REQ", "REP", "DROP", "TIMEOUT"]:
            uuid = frames[1]
            if users.has_key(uuid):
                user = users[uuid]
            else:
                user = User(uuid)
                users[uuid] = user

        if t == "REQ":
            stats["requests"] += 1
            user.request_made()
            
        elif t == "REP":
            stats["replies"] += 1
            reply_time = user.reply_received()
            if reply_time:
                stats["reply_time_sum"] += reply_time
                stats["max_reply_time"] = max(stats["max_reply_time"], reply_time)
                stats["min_reply_time"] = min(stats["min_reply_time"], reply_time)
            
        elif t == "DROP":
            stats["dropped"] += 1
        elif t == "TIMEOUT":
            stats["timeouts"] += 1
            
        elif t == "WORKER_STATS":
            try:
                #print "{} {}".format(frames[0], frames[1:])
                stats["active_workers_sum"] += int(frames[1])
                stats["ready_workers_sum"] += int(frames[2])
                stats["request_queue_length_sum"] += int(frames[3])
                stats["queue_samples"] += 1
                #log_worker_aggregate_stats(int(frames[1]), int(frames[2]), int(frames[3]))
            except ValueError:
                pass
            
        message = " | ".join(frames[1:])
            
        #print "[{}] {}: {}".format(datetime.now(), t, message)
        
    else:
        print "[{}] {}".format(datetime.now(), frames[0])
        
    log_stats()
        

    
zmq_socket.close()
zmq_context.term()