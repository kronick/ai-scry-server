#coding=utf-8

import zmq
import time
import json
import os
import uuid

from flask import Flask, jsonify, request, render_template, send_from_directory

app = Flask(__name__)

config = {
    "UPLOAD_FOLDER": "/home/ubuntu/webapps/U1F52E/uploads/",
    "ZMQ_BROKER_ADDR": "tcp://localhost:5559",
    "ZMQ_RETRIES": 1,
    "ZMQ_REQUEST_TIMEOUT": 10000
}

@app.route('/')
def index():
    out = u""
    for i in range(0,10000):
        out += u"🔮"
    return out
    
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
    
    r = jsonify(response="OK", id=id, captions=captions, caption_time=caption_time, process_time=process_time)
    r.status_code = code
    
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
    
app.debug = True    
if __name__ == '__main__':
    
    app.run()