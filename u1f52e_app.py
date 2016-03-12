#coding=utf-8

import zmq
import time
import json
import os
import uuid
import sqlite3
import math
from datetime import datetime, timedelta

from flask import Flask, jsonify, request, render_template, send_from_directory

app = Flask(__name__)

config = {
    "UPLOAD_FOLDER": "/home/ubuntu/webapps/U1F52E/uploads/",
    "ZMQ_BROKER_ADDR": "tcp://localhost:5559",
    "ZMQ_RETRIES": 1,
    "ZMQ_REQUEST_TIMEOUT": 10000,
    "ZMQ_QUEUE_INFO_ADDR": "tcp://localhost:5411",
    "LOGGING_SERVER_ADDR": "tcp://localhost:5500",
    "LOG_INTERVAL": 10,  # Log stats every 10 seconds
    "LOG_FILENAME": "log.db",
}

@app.route('/')
def index():
#     out = u""
#     for i in range(0,10000):
#         out += u"🔮"
#     return out
    return render_template("index.html")
    
@app.route('/favicon')
@app.route('/favicon.ico')
def favicon():
    return send_from_directory("static", "favicon.png")


    
@app.route('/stats')
def get_stats_page():
    return render_template("stats.html")

@app.route('/stats/data/<start>/<end>')    
@app.route('/stats/data/<start>/<end>/<samples>')
@app.route('/stats/data')
def get_stats_data(start=-1, end=-1, samples=-1):
    if start < 0:
        start = str(datetime.now() + timedelta(hours=-24))
    if end < 0:
        end = str(datetime.now())
    if samples < 0:
        samples = 100
        
    with sqlite3.connect(config["LOG_FILENAME"]) as db:
        db.row_factory = sqlite3.Row
        cursor = db.cursor()
        cursor.execute("SELECT * FROM client_aggregate_stats WHERE timestamp > datetime(?) and timestamp < datetime(?) order by timestamp asc", (start, end))
        
        client_results = [dict(r) for r in cursor.fetchall()]
        
        cursor.execute("SELECT * FROM worker_aggregate_stats WHERE timestamp > datetime(?) and timestamp < datetime(?) order by timestamp asc", (start, end))
        
        worker_results = [dict(r) for r in cursor.fetchall()]
        
        # Average over groups of samples
        def average_client_result(rows):
            timestamps      = [r["timestamp"] for r in rows]
            active_users    = [r["active_users"] for r in rows] 
            avg_waits       = [r["avg_wait"] for r in rows]
            min_waits       = [r["min_wait"] for r in rows]
            max_waits       = [r["max_wait"] for r in rows]
            request_rates   = [r["request_rate"] for r in rows]
            reply_rates     = [r["reply_rate"] for r in rows]
            drop_rates      = [r["drop_rate"] for r in rows]
            timeout_rates   = [r["timeout_rate"] for r in rows]
            
            return {
                "timestamp": timestamps[len(timestamps)/2].replace(" ", "T"),
                "active_users": sum(active_users) / float(len(active_users)),
                "avg_wait": sum(avg_waits) / float(len(avg_waits)),
                "min_wait": min(min_waits),
                "max_wait": max(max_waits),
                "request_rate": sum(request_rates) / float(len(request_rates)),
                "reply_rate": sum(reply_rates) / float(len(reply_rates)),
                "drop_rate": sum(drop_rates) / float(len(drop_rates)),
                "timeout_rate": sum(timeout_rates) / float(len(timeout_rates))
            }
        
        def average_worker_result(rows):
            timestamps          = [r["timestamp"] for r in rows]
            active_workers      = [r["active_workers"] for r in rows] 
            ready_workers       = [r["ready_workers"] for r in rows]
            request_queue_length= [r["request_queue_length"] for r in rows]
            
            return {
                "timestamp": timestamps[len(timestamps)/2].replace(" ", "T"),
                "active_workers": sum(active_workers) / float(len(active_workers)),
                "ready_workers": sum(ready_workers) / float(len(ready_workers)),
                "request_queue_length": sum(request_queue_length) / float(len(request_queue_length)),
            }
            
        def partition(lst, n):
            q, r = divmod(len(lst), n)
            indices = [q*i + min(i, r) for i in xrange(n+1)]
            return [lst[indices[i]:indices[i+1]] for i in xrange(n)]
        
        client_groups = partition(client_results, int(samples))
        client_output = [average_client_result(g) for g in client_groups]
        
        worker_groups = partition(worker_results, int(samples))
        worker_output = [average_worker_result(g) for g in worker_groups]        
    
        
        
        
        return jsonify(client_stats=client_output, worker_stats=worker_output)


@app.route("/stats/userdata")
def get_user_data():
    with sqlite3.connect(config["LOG_FILENAME"]) as db:
        db.row_factory = sqlite3.Row
        cursor = db.cursor()
        cursor.execute("select sum(duration)/60 as total, count(*) as n_sessions,  uuid from user_sessions group by uuid order by total desc;")
        
        user_results = [dict(r) for r in cursor.fetchall()]
        
        unique_users = len(user_results)
        duration_sum = 0
        sessions_sum = 0
        for u in user_results:
            duration_sum += u["total"]
            sessions_sum += u["n_sessions"]
            
        
        return jsonify(users = user_results, stats = {"unique_users": unique_users, "avg_duration": duration_sum / unique_users, "avg_sessions": float(sessions_sum)  / unique_users})


@app.route("/stats/data/workerdata")
def get_worker_data():
    # Prepare ZeroMQ context and sockets
    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.REQ)
    zmq_socket.connect(config["ZMQ_QUEUE_INFO_ADDR"])
    
    zmq_poll = zmq.Poller()
    zmq_poll.register(zmq_socket, zmq.POLLIN)

    try:
        print "Sending ZeroMQ message"
        message = "INFOPLEASE"
        zmq_socket.send(message)
        
        waiting_for_reply = True

        # Poll connections and get status
        socks = dict(zmq_poll.poll(config["ZMQ_REQUEST_TIMEOUT"]))
        if socks.get(zmq_socket) == zmq.POLLIN:
            response = zmq_socket.recv()
    except Exception as z:
        return str(z)   
        
    return response 


@app.route('/stress')
def stress_test():
    """ Interface for stress testing the caption workers """
    return render_template("stress.html")

@app.route('/test/<temp>')
def test_ipc(temp=1.0):
    """ Send out a filename/URL for the neuraltalk2 worker to process """
    # Prepare ZeroMQ context and sockets
    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.REQ)
    zmq_socket.connect(config["ZMQ_BROKER_ADDR"])
    try:
        print "Sending ZeroMQ message"
        #zmq_socket.send("/home/ubuntu/webapps/U1F52E/uploads/jumpsuit.jpg " + str(temp) + " 5")
        zmq_socket.send("/home/ubuntu/webapps/U1F52E/uploads/jumpsuit.jpg {} {}".format(temp, 5))
        response = zmq_socket.recv()
        return response
    except ZMQError as z:
        return z
        
@app.route("/image/<uuid>", methods=["GET"])
def getImage(uuid):
    return send_from_directory(config["UPLOAD_FOLDER"], "{}.jpg".format(uuid))

@app.route("/captionsForImage", methods=["POST"])
def getCaptionsForImage():
    start = time.time()
    print "Processing request."
    
    data = request.get_json()
    # Save image out to file
    id = uuid.uuid4()
    filename = os.path.join(config["UPLOAD_FOLDER"], "{}.jpg".format(id))
    
    send_message_to_logger("REQ", data["uuid"])
    
    with open(filename, "wb") as f:
        f.write(data["image"].decode('base64'))

    #captions = getCaptionsFromNeuraltalk2(filename, data["temperature"], data["n"])
    tic = time.time()
    captions = getCaptionsFromNeuraltalk2("http://u1f52e.net/image/{}".format(id), data["temperature"], data["n"])
    print captions
    caption_time = "{:.2f}".format(time.time() - tic)
    process_time = "{:.2f}".format(time.time() - start)
    
    response = "OK"
    code = 200
    if captions == "The queue is full!":
        response = "The queue is full!"
        captions = ""
        code = 503
        send_message_to_logger("DROP", [data["uuid"], process_time])
    elif captions == "focus...":
        send_message_to_logger("TIMEOUT", [data["uuid"], process_time])
    else:
        send_message_to_logger("REP", [data["uuid"], captions])
    
    r = jsonify(response="OK", id=id, captions=captions, caption_time=caption_time, process_time=process_time)
    r.status_code = code
    
    #log_stats()
    
    return r
    
def getCaptionsFromNeuraltalk2(file, temp, n):
    # Prepare ZeroMQ context and sockets
    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.REQ)
    zmq_socket.connect(config["ZMQ_BROKER_ADDR"])
    
    zmq_poll = zmq.Poller()
    zmq_poll.register(zmq_socket, zmq.POLLIN)
    
    # Based on http://zguide.zeromq.org/py:lpclient
    # Poll socket for a certain amount of time, close connection and retry if no response
    retries_left = config["ZMQ_RETRIES"]
    while retries_left:
        try:
            print "Sending ZeroMQ message"
            message = "{} {} {}".format(file, temp, n)
            zmq_socket.send(message)
            
            waiting_for_reply = True
            while waiting_for_reply:
                # Poll connections and get status
                socks = dict(zmq_poll.poll(config["ZMQ_REQUEST_TIMEOUT"]))
                if socks.get(zmq_socket) == zmq.POLLIN:
                    response = zmq_socket.recv()
                    if not response:
                        break
                    else:
                        # Response received
                        # TODO: perform any error checking here before sending a reply
                        
                        return response.replace("UNK", "(unknown)").replace("a (unknown)", "an (unknown)")
                else:
                    # Socket isn't working for one reason or another, so close it and retry
                    print "Timeout waiting for caption worker response."
                    zmq_socket.setsockopt(zmq.LINGER, 0)
                    zmq_socket.close()
                    zmq_poll.unregister(zmq_socket)
                    retries_left -= 1
                    if retries_left == 0:
                        print "Retried {} times with no luck. Abandoning this request.".format(config["ZMQ_RETRIES"])
                        return "focus..."
                        break
                    
                    print "Retrying..."
                    
                    # Create a new message
                    zmq_socket = zmq_context.socket(zmq.REQ)
                    zmq_socket.connect(config["ZMQ_BROKER_ADDR"])
                    zmq_poll.register(zmq_socket, zmq.POLLIN)
                    zmq_socket.send(message)
                    
        except Exception as z:
            return str(z)

def send_message_to_logger(t, message):
    """ Send a message to the logging server """
    # Prepare ZeroMQ context and sockets
    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.REQ)
    zmq_socket.connect(config["LOGGING_SERVER_ADDR"])
    
    zmq_poll = zmq.Poller()
    zmq_poll.register(zmq_socket, zmq.POLLIN)
    frames = [t]
    if isinstance(message, basestring):
        frames.append(bytes(message.encode("utf-8")))
    else:
        for m in message:
            frames.append(bytes(m.encode("utf-8")))
        
    zmq_socket.send_multipart(frames)
    try:
        socks = dict(zmq_poll.poll(100))
        if socks.get(zmq_socket) == zmq.POLLIN:
            response = zmq_socket.recv()
            if not response:
                return False
            else:
                return True
        else:
            return False
    except Exception as z:
        print str(z)
        return False




app.debug = True    
if __name__ == '__main__':
    
    app.run()