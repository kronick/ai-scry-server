# Based on the Paranoid Pirate queue by Daniel Lundin: http://zguide.zeromq.org/py:ppqueue

from collections import OrderedDict
import time
import sys
import zmq
from zhelpers import socket_set_hwm
from datetime import datetime

from collections import deque

config = {
    "LOGGING_SERVER_ADDR": "tcp://localhost:5500",
    "LOG_INTERVAL": 1,  # Log stats every 1 seconds
}
last_log_timestamp = time.time()

HEARTBEAT_LIVENESS = 10     # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1.0   # Seconds

#  Paranoid Pirate Protocol constants
PPP_READY = "\x01"      # Signals worker is ready
PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat

alive_workers = {}

class Worker(object):
    def __init__(self, address, hostname="???"):
        self.address = address
        self.hostname = hostname
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker):
        """ Move worker from its old place to end of the queue """
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker

        # Update expiration date if exists, add to registry of alive workers if doesn't exist
        if alive_workers.has_key(worker.address):
            alive_workers[worker.address].expiry = worker.expiry
        else:
            alive_workers[worker.address] = worker

    def keepalive(self, address):
        if alive_workers.has_key(address):
            alive_workers[address].expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
        if self.queue.has_key(address):
            self.queue[address].expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []
        for address,worker in alive_workers.iteritems():
            if t > worker.expiry:  # Worker expired
                expired.append(address)
        for address in expired:
            print "W: Idle worker expired: %s" % address
            self.queue.pop(address, None)
            alive_workers.pop(address, None)

    def next(self):
        address, worker = self.queue.popitem(False)
        return address

context = zmq.Context(1)

frontend = context.socket(zmq.ROUTER) # ROUTER
backend = context.socket(zmq.ROUTER)  # ROUTER
info_sock = context.socket(zmq.REP)

socket_set_hwm(frontend, 5)
socket_set_hwm(backend, 5)

frontend.bind("tcp://*:5559") # For clients
backend.bind("tcp://*:5560")  # For workers
info_sock.bind("tcp://*:5411") # for debugging internal state info

poll_workers = zmq.Poller()
poll_workers.register(backend, zmq.POLLIN)

poll_both = zmq.Poller()
poll_both.register(frontend, zmq.POLLIN)
poll_both.register(backend, zmq.POLLIN)
poll_both.register(info_sock, zmq.POLLIN)

workers = WorkerQueue()

heartbeat_at = time.time() + HEARTBEAT_INTERVAL

queue_length = {}
requests = deque()

def log_stats():
    global last_log_timestamp
    dT = time.time() - last_log_timestamp
    if dT < config["LOG_INTERVAL"]:
        return
    
    last_log_timestamp = time.time()
    
    logger = context.socket(zmq.REQ)
    logger.connect(config["LOGGING_SERVER_ADDR"]) # For logging system
    zmq_poll = zmq.Poller()
    zmq_poll.register(logger, zmq.POLLIN)
    
    message = ["WORKER_STATS", "{}".format(len(alive_workers)),  "{}".format(len(workers.queue)), "{}".format(len(requests))]
    logger.send_multipart(message)

    try:
        socks = dict(zmq_poll.poll(100))
        if socks.get(logger) == zmq.POLLIN:
            response = logger.recv()
            if not response:
                return False
            else:
                return True
        else:
            return False
    except Exception as z:
        print str(z)
        return False

print "[{}] Entering main run loop...".format(datetime.now())
while True:
    poller = poll_both
    
    socks = dict(poller.poll(100))
    
    # Handle requests for info
    if socks.get(info_sock) == zmq.POLLIN:
        frames = info_sock.recv_multipart()

        info = ""
        for w in alive_workers:
            info += "{}: {}, ".format(w, alive_workers[w].hostname)
        
        info_sock.send(info)

    # Handle worker activity on backend
    if socks.get(backend) == zmq.POLLIN:
        # Use worker address for LRU routing
        frames = backend.recv_multipart()
        if not frames:
            break

        address = frames[0]
        msg = frames[1:]
           
        # Validate control message, or return reply to client
        if len(msg) == 1:
            if msg[0] not in (PPP_READY, PPP_HEARTBEAT):
                print "[{}] E: Invalid message from worker: {}".format(datetime.now(), msg)
            elif msg[0] == PPP_READY:
                print "[{}] Worker {} is now online!".format(datetime.now(), address)
                workers.ready(Worker(address))
            elif msg[0] == PPP_HEARTBEAT:
                #print "[{}] Worker {} heartbeat".format(datetime.now(), address)
                if not alive_workers.has_key(address):
                    workers.ready(Worker(address))
                else:
                    workers.keepalive(address)
        elif msg[0] == PPP_READY:
            hostname = msg[1]
            print "[{}] Worker {} is now online! (Host: {})".format(datetime.now(), address, hostname)
            workers.read(Worker(address, hostname))
        elif msg[0] == PPP_HEARTBEAT:
            hostname = msg[1]
            if not alive_workers.has_key(address):
                workers.ready(Worker(address, hostname))
            else:
                workers.keepalive(address)

        else:
            print "[{}] Received response from worker ID {}".format(datetime.now(), address)

            frontend.send_multipart(msg)
            print msg[2]
            print "From worker: {}".format("-".join([str(ord(c)) for c in frames[0]]))
            
            workers.ready(Worker(address))


        # Send heartbeats to idle workers if it's time
        if time.time() >= heartbeat_at:
            #print "Sending heartbeat to workers..."
            for worker in alive_workers:
                msg = [worker, PPP_HEARTBEAT]
                backend.send_multipart(msg)
            heartbeat_at = time.time() + HEARTBEAT_INTERVAL

    # Handle client request on the frontend
    if socks.get(frontend) == zmq.POLLIN:
        frames = frontend.recv_multipart()
        if not frames:
            break
        
        if len(requests) >= len(alive_workers):
            # Return an error that the queue is full
            response = [frames[0], b'', "The queue is full!"]
            frontend.send_multipart(response)
            print "[{}] Queue is full! Dropping request.".format(datetime.now())
        else:
            requests.appendleft(frames)

        print("[{}] Queue is {} request long. {}/{} workers ready.".format(datetime.now(), len(requests), len(workers.queue), len(alive_workers)))
    
    if(len(requests) > 0):
        print "[{}] Processing queue... {} workers available".format(datetime.now(), len(workers.queue))

    while len(workers.queue) > 0 and len(requests) > 0:
        frames = requests.popleft()
        
        who_gets_this_one = workers.next()
        frames.insert(0, who_gets_this_one)
        
        print "[{}] Passing message to worker ID {} ({} available)".format(datetime.now(), who_gets_this_one, len(workers.queue))

        backend.send_multipart(frames)
        
        if len(workers.queue) == 0:
            print "[{}] No more workers available! Adding to the queue...".format(datetime.now())
            break

    workers.purge()
    log_stats()
    sys.stdout.flush()