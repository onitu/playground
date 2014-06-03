#!/usr/bin/env python

import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect ("tcp://localhost:{}".format(53487))

socket.send(b'Hello')
print(socket.recv())
