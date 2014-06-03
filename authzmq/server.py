#!/usr/bin/env python

import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
#socket.bind("tcp://*:{}".format(53487))
socket.bind("tcp://127.0.0.1:{}".format(53487))

while True:
    msg = socket.recv()
    print("Received request: ", msg)
    socket.send(b'ok')
