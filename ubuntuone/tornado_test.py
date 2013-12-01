import socket
import tornado
import tornado.ioloop
import tornado.iostream
from tornado import gen

from urlparse import urlparse, parse_qsl
from oauthlib.oauth1 import Client, SIGNATURE_PLAINTEXT, SIGNATURE_TYPE_QUERY
import json, protocol_pb2, struct

import U1Requests

class U1Handler:
    FMT_MSG_SIZE = "!I" # In U1 protocol, peers always send the size of a message before sending the message itself. It is the format string to use to get that message size.
    HOST, PORT = ("fs-1.one.ubuntu.com", 443) # The U1 server we're connecting to and the port

    def __init__(self):
        self.volumes = []
        # TODO load the volumes here
        self.msg_id = 1 # we are a client, we send odd requests
        self.requests = {} # Dict of our pending requests. Key is the msg_id

    def connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(0)
        self.stream = tornado.iostream.SSLIOStream(sock) # Let Tornado handle the ssl stuff for us
        self.stream.connect((U1Handler.HOST, U1Handler.PORT), self.initialize_connection)
        self.io_loop = tornado.ioloop.IOLoop.instance()
        self.io_loop.start()

    ### Data transport & waiting methods

    @gen.coroutine
    def send_request(self, req):
        """Function used to finalize a protobuf sending.
        Sets the id of the protobuf and increment the class' msg_id.
        Sets a callback in wait for the response."""
        message = req.trigger.SerializeToString()
        # send the message length (according to protocol)
        msgLength = struct.pack(U1Handler.FMT_MSG_SIZE, len(message))
        self.stream.write(msgLength)
        # send the actual message, and set a callback for the response
        self.stream.write(message, self.receive_message)
        self.receive_message() # wait for the answer

    def process_message(self, data):
        msg = protocol_pb2.Message()
        msg.ParseFromString(data)
        if msg.id % 2 != 0: # Odd = it's a response to one of our requests
            try:
                self.requests[msg.id].process(msg)
            except KeyError: # should not happen
                print 'Server Error', msg.id
                self.io_loop.stop()
        else: # Even : unsolicited message from the server !
            pass # nothing atm
        self.receive_message() # wait for a new message

    @gen.coroutine
    def receive_message(self):
        msgLength = yield gen.Task(self.stream.read_bytes, struct.calcsize(U1Handler.FMT_MSG_SIZE))
        msgLength = struct.unpack(U1Handler.FMT_MSG_SIZE, msgLength)[0]
        data = yield gen.Task(self.stream.read_bytes, msgLength)
        self.process_message(data)

    def request_finished(self, req_id):
        del self.requests[req_id]

    ### Data transfer functions
    @gen.coroutine
    def initialize_connection(self):
        greeting = yield gen.Task(self.stream.read_until, b"\r\n")
        print greeting # We really don't care about the greeting
        self.authenticate()
#        self.list_volumes()

    def authenticate(self):
        auth_request = U1Requests.AuthRequest(self)
        self.send_request(auth_request)

    def test(self):
        print 'test'

def msg_type(msg):
    return protocol_pb2.Message.DESCRIPTOR.enum_types_by_name['MessageType'].values_by_number[msg.type].name

handler = U1Handler()

def test_entrypoint():
    global handler
    
    handler.test()

def start():
    global handler
    handler.connect()
    
if __name__ == "__main__":
    start()
