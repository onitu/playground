import protocol_pb2, json
from urlparse import urlparse, parse_qsl
from oauthlib.oauth1 import Client, SIGNATURE_PLAINTEXT, SIGNATURE_TYPE_QUERY

class RequestException(Exception):
  """Something's gone wrong during a request"""
  def __init__(self, what):
    self.what = what


class U1Request:
    def __init__(self, handler):
      handler.requests[handler.msg_id] = self
      self.req_id = handler.msg_id
      handler.msg_id += 2 # to stay odd
      self.handler = handler # the u1 handler
      self.responses = [] # the stored responses to the request
      self.counter = 0 # response counter. useful to know "when" a response has been sent in the process. We cannot only rely on len(self.responses), because since some responses are useless to store, they would be deleted right away 
      self.states = {} # A request kind of works like a state machine. Stores the different states the request can be in
      self.message = message = protocol_pb2.Message() # create a new protobuf message
      self.message.id = self.req_id # give it the right id
      self.initialize() # for further message initialization

    
    def initialize(self):
      """Implementing this is up to the inheriting request class"""
      raise NotImplementedError()


    def process(self, msg):
      """Method called when the server sent a response to our request.
      What to do is up to the inheriting request class."""
      self.responses.append(msg)
      self.counter += 1
      if msg.type == protocol_pb2.Message.ERROR:
        pass # TODO: a server error occurred, what do we do ?
      else:
        try:
          self.states[msg.type](msg) # call
          if msg.type == self.end_msg_type:
            self.finish()
        except KeyError:
          pass # TODO: the server sent an unexpected response to our request. what do we do ?
        except RequestException:
          pass # TODO: something went wrong when dealing with the response. what do we do ?

    
    def finish(self):
      """What we do upon a request is finished"""
      self.handler.request_finished(self.req_id) # the request deletes itself of the handler dict


    def msg_type(self, msg):
      """Returns the string representation of the message id's protobuf enum name."""
      return protocol_pb2.Message.DESCRIPTOR.enum_types_by_name['MessageType'].values_by_number[msg.type].name


    
class AuthRequest(U1Request):
  """AUTH_REQUEST request class."""

  CREDENTIALS_FILE = ".credentials" # Our OAuth credentials file


  def __init__(self, handler):
    U1Request.__init__(self, handler)
    self.states[protocol_pb2.Message.AUTH_AUTHENTICATED] = self.authenticated
    self.states[protocol_pb2.Message.ROOT] = self.root
    self.end_msg_type = protocol_pb2.Message.ROOT # the type of message signing the end of request


  def initialize(self):
    """Retrieves the OAuth credentials."""
    # retrieve our OAuth credentials from file
    def credentials_from_file():
      """Extracts the OAuth credentials from file"""
      with open(AuthRequest.CREDENTIALS_FILE) as f:
        jsoncreds = json.loads(f.read())
        return jsoncreds

    oauth_creds = credentials_from_file()
    self.message.type = protocol_pb2.Message.AUTH_REQUEST
    # Signs the message with oauth
    client = Client(oauth_creds["consumer_key"], oauth_creds["consumer_secret"],
                    oauth_creds["token"], oauth_creds["token_secret"],
                    signature_method=SIGNATURE_PLAINTEXT,
                    signature_type=SIGNATURE_TYPE_QUERY)
    url, headers, body = client.sign('http://server')
    # Parse out the authentication parameters from the query string.
    auth_parameters = dict((name, value) for name, value in
                           parse_qsl(urlparse(url).query)
                           if name.startswith('oauth_'))
    # add the authentication informations to the protobuf
    for key, value in auth_parameters.items():
      newparam = self.message.auth_parameters.add() # create a new parameter
      newparam.name = key
      newparam.value = value


  def authenticated(self, msg):
    if self.counter != 1: # To an AUTH_REQUEST request, ROOT should come second
      raise RequestException("Server didn't send the good response in expected time (protocol error?")
    print 'Authentication OK, session id:', msg.session_id
    self.handler.session_id = msg.session_id # keep it for log purposes


  def root(self, msg):
    if self.counter != 2: # To an AUTH_REQUEST request, ROOT should come second
      raise RequestException("Server didn't send the good response in expected time (protocol error?")
    root = msg.root
    print 'ROOT: Node', root.node, 'generation:', root.generation, 'free bytes:', root.free_bytes    


class ListVolumes(U1Request):
  """LIST_VOLUMES request class."""


  def __init__(self, handler):
    U1Request.__init__(self, handler)
    self.states[protocol_pb2.Message.VOLUMES_INFO] = self.volumes_info
    self.states[protocol_pb2.Message.VOLUMES_END] = self.volumes_end
    self.end_msg_type = protocol_pb2.Message.VOLUMES_END # the type of message signing the end of request


  def initialize(self):
    """Ask the server to list the current volumes."""
    self.message.type = protocol_pb2.Message.LIST_VOLUMES


  def volumes_info(self, msg):
    print 'volume info:'
    volume_info = msg.list_volumes
    if volume_info.type == protocol_pb2.Volumes.ROOT:
      print 'ROOT node:', print volume_info.root.node, 'generation:', volume_info.root.generation, 'free bytes:', volume_info.root.free_bytes
    elif volume_info.type == protocol_pb2.Volumes.UDF:
      print 'User Defined Folder:', volume_info.udf.volume, 'node:', volume_info.udf.node, 'suggested path:', volume_info.udf.suggested_path, 'generation:', volume_info.udf.generation, 'free bytes:', volume_info.udf.free_bytes
    elif volume_info.type == protocol_pb2.Volumes.SHARE:
      print 'Share'
      # Shares are a bit more complicated. Not managed yet


  def volumes_end(self, msg):
    print 'End of the volumes listing'



# class ProtocolRequest(U1Request):
#   """PROTOCOL_VERSION request class."""


#   def __init__(self, handler):
#     U1Request.__init__(self, handler)
#     self.states[protocol_pb2.Message.PROTOCOL_VERSION] = self.protocol_version
#     self.end_msg_type = protocol_pb2.Message.PROTOCOL_VERSION # the type of message signing the end of request


#   def initialize(self):
#     """Ask the server what protocol version is it running."""
#     message = protocol_pb2.Message()
#     message.id = self.req_id
#     message.type = protocol_pb2.Message.PROTOCOL_VERSION
#     self.message = message # don't forget to store the created message in self ! for it to be sent

#   def protocol_version(self, msg):
#     print 'version:', msg.protocol.version
    

