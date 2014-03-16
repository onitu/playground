import socket, json, struct
from urlparse import urlparse, parse_qsl
import tornado, tornado.ioloop, tornado.iostream
from tornado import gen
import protocol_pb2
import U1Requests

class U1Handler:
    FMT_MSG_SIZE = "!I" # In U1 protocol, size of a message is sent before the message itself. It is the format string to use to get that message size
    HOST, PORT = ("fs-1.one.ubuntu.com", 443) # The U1 server we're connecting to and the port
    ROOT_VOLUME_PATH = u'~/Ubuntu One' # The standard root path 

    def __init__(self):
        self.free_bytes = 0
        self.volumes = {}
        # TODO load the saved info here
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
        Sends the actual message preceded by its length in bytes."""
        message = req.message.SerializeToString()
        # send the message length (according to protocol)
        msgLength = struct.pack(U1Handler.FMT_MSG_SIZE, len(message))
        self.stream.write(msgLength)
        # send the actual message, and set a callback for the response
        self.stream.write(message, self.receive_message)


    def process_message(self, data):
        """Function called when a message is received"""
        msg = protocol_pb2.Message()
        msg.ParseFromString(data)
        # print 'Servent sent message id', msg.id, 'type', msg.type, 'msg:', msg
        if msg.id % 2 != 0: # Odd = it's a response to one of our requests
            try:
                self.requests[msg.id].process(msg)
            except KeyError as ke: # should not happen because it always should be in response to one of our previous requests
                print 'Server error on message : {} ({})'.format(msg, ke),  # TODO: what to do ?
                self.io_loop.stop()
            except U1Requests.RequestException as re: # The request couldn't handle the response
                print 'Request exception: {}'.format(re)
                self.io_loop.stop()
            except Exception as e: # whatever happened
                print 'Unhandled exception: {}'.format(e)
                self.io_loop.stop()
        else: # Even : it's an unsolicited message from the server
            # call the function handling this kind of messages
            req_name = U1Requests.msg_type(msg)
            handler = getattr(self, "handle_" + req_name, None)
            if handler is not None:
                handler(msg)
            else: # Not supposed to happen (obsolete protocol ?)
                raise Exception("Cant handle message '{}' {}".format(U1Requests.msg_type(msg), str(message).replace("\n", " ")))
        self.receive_message() # wait for a new message


    @gen.coroutine
    def receive_message(self):
        """Function used to receive a message. First reads the amount of data of the message (in bytes, as an unsigned integer),
        then the message itself and passes it on to process_message."""
        msgLength = yield gen.Task(self.stream.read_bytes, struct.calcsize(U1Handler.FMT_MSG_SIZE))
        msgLength = struct.unpack(U1Handler.FMT_MSG_SIZE, msgLength)[0]
        data = yield gen.Task(self.stream.read_bytes, msgLength)
        self.process_message(data)


    ### Data transfer functions
    @gen.coroutine
    def initialize_connection(self):
        """First contact with the Ubuntu One server.
        Reads the server greeting, authenticates ourselves, list the current volumes on the server,
        then asks it a delta from the last generation we know of"""
        greeting = yield gen.Task(self.stream.read_until, b"\r\n") # get the server greeting
        print greeting # whatever (we don't care about the greeting)
        self.send_request(U1Requests.AuthRequest(self)) # First mandatory step : OAuth authentication
        self.send_request(U1Requests.ListVolumes(self, callback=self.update_volumes)) # check the server's volumes list
        self.send_request(U1Requests.GetContent(self,
                                                share='2867b9fd-5cac-4d01-9c72-be2f769b7429',
                                                node='7a118492-d207-4ba3-aa69-76f8f93541eb',
                                                hash='sha1:03f7495b51cc70b76872ed019d19dee1b73e89b6',
                                                offset=1,
                                                filename='test')) # check the server's volumes list
        print 'Initialization complete, volumes list:'
        print self.volumes

    def update_volume_generation(self, vol_id, delta_end):
        """Updates a volume generation."""
        self.volumes[vol_id]['generation'] = delta_end.generation


    def update_volumes(self, list_volumes):
        """Callback function called at initialization when the server's volumes listing is complete.
        For each listed volume, will update the informations."""
        volumes = list_volumes.volumes
        for vol_id in volumes:
            self.update_volume_info(volumes[vol_id], vol_id)


    def update_volume_info(self, vol_info, vol_id):
        """Updates our info about a volume. If the given volume is unknown, creates a new entry.
        If the current generation is greater than the one we know, ask a delta to know what has changed."""
        # Retrieve the current generation we're aware of to know later if we are up-to-date.
        try:
            old_gen = self.volumes[vol_id]['generation']
        except KeyError: # No such volume id -> create a New volume
            self.volumes[vol_id] = {}
            old_gen = -1 # special value for the GetDelta request to mean "delta from scratch".
        # Path is special: root doesn't have a "suggested path" field, so we must deal with it.
        # The root path has always been something specific. (TODO: check how it works on Windows)
        if vol_info.type == protocol_pb2.Volumes.ROOT:
            volume = vol_info.root
            self.volumes[vol_id]['path'] = U1Handler.ROOT_VOLUME_PATH
        elif vol_info.type == protocol_pb2.Volumes.UDF:
            volume = vol_info.udf
            self.volumes[vol_id]['path'] = volume.suggested_path
        self.volumes[vol_id]['type'] = vol_info.type
        self.volumes[vol_id]['node'] = volume.node
        self.volumes[vol_id]['free_bytes'] = volume.free_bytes
        if 'generation' not in self.volumes[vol_id] or self.volumes[vol_id]['generation'] < volume.generation:
            # if the reported generation is further than the one we have : demand a delta
            self.send_request(U1Requests.GetDelta(self,
                                                  volume=vol_id,
                                                  from_generation=old_gen,
                                                  on_delta_info=self.update_file_info,
                                                  on_delta_end=self.update_volume_generation))


    def update_file_info(self, delta_info):
        """Updates the information we have about a volume's file.
        Called upon a DELTA_INFO to update ourselves according to the contents of the delta sent by the server.
        Here, "file" means an U1 "node", so it can be a simple file or a directory."""        
        file_info = delta_info.file_info

        vol = file_info.share # share = volume in u1
        node = file_info.node # the file uuid
        if node not in self.volumes[vol] or delta_info.generation > self.volumes[vol][node]:
            self.volumes[vol][node] = {field[0].name: field[1] for field in file_info.ListFields()}


    ### Handling functions of unsolicited events
    def handle_PING(self, msg):
        """Function called upon a PING request from the server. Sends a PONG."""
        pong = U1Requests.Pong(self)
        self.send_request(pong)
        print 'Sent a PONG'
        pong.finish() # doesn't wait for a response, so explicitly delete it


handler = U1Handler()

def start():
    global handler
    handler.connect()


if __name__ == "__main__":
    start()
