# Based on the Paranoid Pirate queue by Daniel Lundin: http://zguide.zeromq.org/py:ppqueue

from collections import OrderedDict
import time
import sys
import zmq
from zhelpers import socket_set_hwm
from datetime import datetime

from collections import deque

HEARTBEAT_LIVENESS = 10     # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1.0   # Seconds

#  Paranoid Pirate Protocol constants
PPP_READY = "\x01"      # Signals worker is ready
PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat

alive_workers = {}

class Worker(object):
    def __init__(self, address):
        self.address = address
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
socket_set_hwm(frontend, 5)
socket_set_hwm(backend, 5)

frontend.bind("tcp://*:5559") # For clients
backend.bind("tcp://*:5560")  # For workers


poll_workers = zmq.Poller()
poll_workers.register(backend, zmq.POLLIN)

poll_both = zmq.Poller()
poll_both.register(frontend, zmq.POLLIN)
poll_both.register(backend, zmq.POLLIN)

workers = WorkerQueue()

heartbeat_at = time.time() + HEARTBEAT_INTERVAL

queue_length = {}
requests = deque()

print "[{}] Entering main run loop...".format(datetime.now())
while True:
    #if len(workers.queue) > 0:
    #    poller = poll_both
    #else:
    #    poller = poll_workers
    poller = poll_both
    
    #socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
    socks = dict(poller.poll(100))

    #print "[{}] Polling...".format(datetime.now())
    # Handle worker activity on backend
    if socks.get(backend) == zmq.POLLIN:
        # Use worker address for LRU routing
        frames = backend.recv_multipart()
        if not frames:
            break

        address = frames[0]
        msg = frames[1:]
        
#         if (msg[0] == PPP_READY and len(msg) == 1) or len(msg) > 1:
#             # Only say this worker is ready if it's a relevant message
#             workers.ready(Worker(address))

           
        # Validate control message, or return reply to client
        if len(msg) == 1:
            if msg[0] not in (PPP_READY, PPP_HEARTBEAT):
                print "[{}] E: Invalid message from worker: {}".format(datetime.now(), msg)
            elif msg[0] == PPP_READY:
                print "[{}] Worker {} is now online!".format(datetime.now(), address)
                workers.ready(Worker(address))
            elif msg[0] == PPP_HEARTBEAT:
                print "[{}] Worker {} heartbeat".format(datetime.now(), address)
                if not alive_workers.has_key(address):
                    workers.ready(Worker(address))
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
    
    sys.stdout.flush()