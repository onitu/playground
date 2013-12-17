import socket, json, struct
from urlparse import urlparse, parse_qsl
import tornado, tornado.ioloop, tornado.iostream
from tornado import gen
import protocol_pb2
import U1Requests

class U1Handler:
    FMT_MSG_SIZE = "!I" # In U1 protocol, size of a message is sent before the message itself. It is the format string to use to get that message size
    HOST, PORT = ("fs-1.one.ubuntu.com", 443) # The U1 server we're connecting to and the port

    def __init__(self):
        self.volumes = []
        # TODO load the saved info about volumes here
        self.msg_id = 1 # we are a client, we send odd requests, starting at 1
        self.requests = {} # Dict of our pending requests. The message id is the key


    def connect(self):
        """Start of the tornado ioloop. Base of everything"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(0)
        self.stream = tornado.iostream.SSLIOStream(sock) # Let Tornado handle the ssl stuff for us
        self.stream.connect((U1Handler.HOST, U1Handler.PORT), self.initialize_connection)
        self.io_loop = tornado.ioloop.IOLoop.instance()
        self.io_loop.start()


    def send_request(self, req):
        """Function used to finalize a protobuf sending.
        Sets the id of the protobuf and increment the class' msg_id.
        Sets a callback in wait for the response."""
        print 'sending req', req.req_id
        message = req.message.SerializeToString()
        # send the message length (according to protocol)
        msgLength = struct.pack(U1Handler.FMT_MSG_SIZE, len(message))
        self.stream.write(msgLength)
        # send the actual message, and set a callback for the response
        self.stream.write(message, self.receive_message)


    def process_message(self, data):
        msg = protocol_pb2.Message()
        msg.ParseFromString(data)
        if msg.id % 2 != 0: # Odd = it's a response to one of our requests
            try:
                self.requests[msg.id].process(msg)
            except KeyError: # should not happen
                print 'Server Error', msg.id # TODO: what do we do ?
                self.io_loop.stop()
            except RequestException as re: # The server didn't send the expected request
                pass # TODO: don't know what to do by now. maybe send an ERROR message ?
        else: # Even : unsolicited message from the server !
            pass # TODO: nothing atm.
        self.receive_message() # wait for a new message


    @gen.coroutine
    def receive_message(self):
        msgLength = yield gen.Task(self.stream.read_bytes, struct.calcsize(U1Handler.FMT_MSG_SIZE))
        msgLength = struct.unpack(U1Handler.FMT_MSG_SIZE, msgLength)[0]
        data = yield gen.Task(self.stream.read_bytes, msgLength)
        self.process_message(data)


    def request_finished(self, req_id):
        """Function called by a request when it's finished, to delete itself of the requests dict."""
        del self.requests[req_id]


    ### Data transfer functions
    @gen.coroutine
    def initialize_connection(self):
        """First contact with the Ubuntu One server.
        Reads the server greeting, authenticates ourselves, list the current volumes on the server,
        then asks it a delta from the last generation we know of"""
        greeting = yield gen.Task(self.stream.read_until, b"\r\n") # get the server greeting
        print greeting # whatever (we don't care about the greeting)
        self.send_request(U1Requests.AuthRequest(self)) # First mandatory step : OAuth authentication
        self.send_request(U1Requests.ListVolumes(self)) # check the server's volumes list


    def authenticate(self):
        """First mandatory step of the U1 connection.
        We create a authentication request by sending our OAuth credentials.
        The server should respond with AUTH_AUTHENTICATED and ROOT commands."""
        auth_request = U1Requests.AuthRequest(self)
        self.send_request(auth_request)


    def list_volumes(self):
        """Ask the server about the current registered Ubuntu One volumes.
        We need to check if there are new volumes that appeared since the last time."""
        list_volumes

handler = U1Handler()

def start():
    global handler
    handler.connect()


if __name__ == "__main__":
    start()
