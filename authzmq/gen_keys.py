#!/usr/bin/env python

import zmq.auth

keys_dir = 'keys'
zmq.auth.create_certificates(keys_dir, 'server')
zmq.auth.create_certificates(keys_dir, 'client')
