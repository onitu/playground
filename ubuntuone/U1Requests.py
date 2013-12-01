import protocol_pb2, json
from urlparse import urlparse, parse_qsl
from oauthlib.oauth1 import Client, SIGNATURE_PLAINTEXT, SIGNATURE_TYPE_QUERY

class RequestException(Exception):
  """Something's gone wrong during a request"""
  def __init__(self, what):
    self.what = what


class U1Request:
    def __init__(self, handler, end_msg_type, ):
        self.req_id = handler.msg_id
        handler.requests[handler.msg_id] = self
        handler.msg_id += 2 # to stay odd
        self.end_msg_type = end_msg_type
        self.responses = []
        self.handler = handler
        self.trigger = self.initialize()

    def assert_msg_type(self, msg, expected_type):
        try:
            assert msg.type == expected_type
        except AssertionError:
            raise RequestException("Server didn't send expected answer")

    def process(self, handler):
        raise NotImplementedError()
    
    def finish(self):
        raise NotImplementedError()
    
    def initialize(self):
        raise NotImplementedError()
    
class AuthRequest(U1Request):
    CREDENTIALS_FILE = ".credentials" # Our OAuth credentials file

    def __init__(self, handler):
        U1Request.__init__(self, handler, protocol_pb2.Message.ROOT)

    def initialize(self):
        # retrieve our OAuth credentials from file
        oauth_creds = self.credentials_from_file()
        message = protocol_pb2.Message()
        message.id = self.req_id
        message.type = protocol_pb2.Message.AUTH_REQUEST
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
            newparam = message.auth_parameters.add() # create a new parameter
            newparam.name = key
            newparam.value = value
        return message

    def process(self, msg):
        self.responses.append(msg)
        if msg.type == protocol_pb2.Message.ERROR:
            pass # a server error occurred
        else:
            if len(self.responses) == 1:
                self.assert_msg_type(msg, protocol_pb2.Message.AUTH_AUTHENTICATED)
                print 'Authentication OK'
            elif len(self.responses) == 2:
                self.assert_msg_type(msg, protocol_pb2.Message.ROOT)
                self.finish()

    def finish(self):
        root = self.responses[1].root
        print 'ROOT: Node', root.node, 'generation:', root.generation, 'free bytes:', root.free_bytes
        self.handler.request_finished(self.req_id)

    def credentials_from_file(self):
        """Extracts the OAuth credentials from file"""
        with open(AuthRequest.CREDENTIALS_FILE) as f:
            jsoncreds = json.loads(f.read())
        return jsoncreds

