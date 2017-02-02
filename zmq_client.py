import zmq
# Prepare ZeroMQ context and sockets
zmq_context = zmq.Context()
zmq_socket = zmq_context.socket(zmq.REQ)    
zmq_socket.connect("tcp://localhost:5559")

while True:
    try:
        zmq_socket.send(b"Are you there?")
        response = zmq_socket.recv()
        print "Received: " + response
    except ZMQError as e:
        print e