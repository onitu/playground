#!/usr/bin/env python

import zmq
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator

context = zmq.Context()
auth = ThreadAuthenticator(context)
auth.start()
#auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)
auth.configure_curve(domain='*', location='authorized_keys')

socket = context.socket(zmq.REP)
socket.curve_server = True
socket.curve_publickey, socket.curve_secretkey = zmq.auth.load_certificate('keys/server.key_secret')
socket.bind("tcp://*:{}".format(53487))

while True:
    msg = socket.recv()
    print("Received request: ", msg)
    socket.send(b'ok')

auth.stop()
