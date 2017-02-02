import zmq
import time
# Prepare ZeroMQ context and sockets
zmq_context = zmq.Context()
zmq_socket = zmq_context.socket(zmq.REP)    
zmq_socket.bind("tcp://*:5559")

while True:
    try:
        response = zmq_socket.recv()
        print "Message received: " + response
        time.sleep(1)
        zmq_socket.send(b"Message received")
    except ZMQError as e:
        print e