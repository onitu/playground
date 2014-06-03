#!/usr/bin/env python

import zmq
import zmq.auth

context = zmq.Context()

socket = context.socket(zmq.REQ)
socket.curve_publickey, socket.curve_secretkey = zmq.auth.load_certificate('keys/client.key_secret')
socket.curve_serverkey, _ = zmq.auth.load_certificate('keys/server.key')
socket.connect ("tcp://localhost:{}".format(53487))

socket.send(b'Hello')
print(socket.recv())
