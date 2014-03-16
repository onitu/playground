#!/usr/bin/python
# -*- coding: utf-8 -*-

import json, zlib
from datetime import date
from urlparse import urlparse, parse_qsl
from oauthlib.oauth1 import Client, SIGNATURE_PLAINTEXT, SIGNATURE_TYPE_QUERY
import protocol_pb2

class RequestException(Exception):
  """Something's gone wrong during a request"""
  def __init__(self, what):
    self.what = what


class U1Request:
    def __init__(self, handler, kwargs):
      self.handler = handler # the u1 requests handler
      self.handler.requests[self.handler.msg_id] = self
      # Setting a function to call when the request is complete.
      try:
        self.callback = kwargs['callback']
      except KeyError: # No callback given: do nothing
        self.callback = None
      # If the request has been created from an unsolicited server message, take it
      if 'unsolicited' in kwargs:
        self.message = kwargs['server_msg'] # the server message
      else: # Otherwise, initialize a new protobuf message
        self.message = protocol_pb2.Message()
        self.message.id = self.handler.msg_id
        self.handler.msg_id += 2 # to stay odd !
      self.responses = []
      self.states = {} # Dict of message type -> function to call
      self.counter = 0 # Response counter

    
    def initialize(self):
      """Implementing this is up to the inheriting request class"""
      raise NotImplementedError()


    def process(self, msg):
      """Method called when the server sent a response to our request.
      What to do is up to the inheriting request class."""
      self.responses.append(msg)
      self.counter += 1
      self.states[msg.type](msg) # call
      if msg.type == self.end_msg_type:
        self.finish()


    def finish(self):
      """What we do upon a request is finished"""
      if self.callback is not None:
        self.callback(self)
      del self.handler.requests[self.message.id]  # the request deletes itself of the handler dict


    
class AuthRequest(U1Request):
  """AUTH_REQUEST request class.
  A request proving our identity to the server via OAuth credentials.
  Must be sent before anything else."""

  CREDENTIALS_FILE = ".credentials" # Our OAuth credentials file

  def __init__(self, handler, **kwargs):
    U1Request.__init__(self, handler, kwargs)
    self.message.type = protocol_pb2.Message.AUTH_REQUEST
    self.states = {protocol_pb2.Message.AUTH_AUTHENTICATED: self.authenticated,
                   protocol_pb2.Message.ROOT: self.root}
    self.end_msg_type = protocol_pb2.Message.ROOT
    # retrieve our OAuth credentials from file
    def credentials_from_file():
      """Extracts the OAuth credentials from file"""
      with open(AuthRequest.CREDENTIALS_FILE) as f:
        jsoncreds = json.loads(f.read())
        return jsoncreds

    oauth_creds = credentials_from_file()
    # Sign the message with oauth
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
    if self.counter != 1: # To an AUTH_REQUEST request, the AUTH_AUTHENTICATED should come first
      raise RequestException("Server didn't send the good response in expected time (protocol error?")
    print 'Authentication OK, session id:', msg.session_id
    self.handler.session_id = msg.session_id # keep it for log purposes


  def root(self, msg):
    if self.counter != 2: # To an AUTH_REQUEST request, ROOT should come second
      raise RequestException("Server didn't send the good response in expected time (protocol error?")
    root = msg.root
    print 'ROOT: Node', root.node, 'generation:', root.generation, 'free bytes:', root.free_bytes    


class ListVolumes(U1Request):
  """LIST_VOLUMES request class.
  A request to ask the server to list all the volumes it's aware of.
  The responses come in a following of VOLUMES_INFO messages with each containing infos about a volume.
  The responses end with a VOLUMES_END message."""
  ROOT_DEFAULT_PATH = u'~/Ubuntu One' # the root usually has this name

  def __init__(self, handler, **kwargs):
    U1Request.__init__(self, handler, kwargs)
    self.message.type = protocol_pb2.Message.LIST_VOLUMES
    self.states[protocol_pb2.Message.VOLUMES_INFO] = self.volumes_info
    self.states[protocol_pb2.Message.VOLUMES_END] = self.volumes_end
    self.end_msg_type = protocol_pb2.Message.VOLUMES_END # the type of message signing the end of request
    self.volumes = {} # Stores every volume information we receive.


  def volumes_info(self, msg):
    """Function called when receiving a VOLUMES_INFO message."""
    print 'VOLUMES_INFO:'
    volume_info = msg.list_volumes
    if volume_info.type == protocol_pb2.Volumes.ROOT:
      volume = volume_info.root
      vol_id = ''
      print 'ROOT node:', volume_info.root.node, 'generation:', volume_info.root.generation, 'free bytes:', volume_info.root.free_bytes
    elif volume_info.type == protocol_pb2.Volumes.UDF:
      volume = volume_info.udf
      vol_id = volume_info.udf.volume
      print 'User Defined Folder:', volume_info.udf.volume, 'node:', volume_info.udf.node, 'suggested path:', volume_info.udf.suggested_path, 'generation:', volume_info.udf.generation, 'free bytes:', volume_info.udf.free_bytes
    elif volume_info.type == protocol_pb2.Volumes.SHARE:
      print 'Share: ignored' # Shares are a bit more complicated. Not managed yet
      pass
    self.volumes[vol_id] = volume_info


  def volumes_end(self, msg):
    """Function called upon a VOLUMES_END message. End of the listing."""
    print 'End of the volumes listing'
    print ''



class GetDelta(U1Request):
  """GET_DELTA request class.
  A request to ask "what's changed ?" to the server since a given generation.
  The responses are a following of DELTA_INFO messages containing file infos about files that changed.
  The responses are finished by a DELTA_END message."""

  def __init__(self, handler, **kwargs):
    U1Request.__init__(self, handler, kwargs)
    self.message.type = protocol_pb2.Message.GET_DELTA
    self.states[protocol_pb2.Message.DELTA_INFO] = self.delta_info
    self.states[protocol_pb2.Message.DELTA_END] = self.delta_end
    self.end_msg_type = protocol_pb2.Message.DELTA_END # the type of message signing the end of request
    # Keep trace of what volume the delta is about.
    self.volume = kwargs['volume']
    # Defining a callback to call each time we receive a DELTA_INFO message.
    try:
      self.on_delta_info = kwargs['on_delta_info']
    except KeyError:
      self.on_delta_info = None
    # Defining a callback to call each time we receive a DELTA_END message.
    try:
      self.on_delta_end = kwargs['on_delta_end']
    except KeyError:
      self.on_delta_end = None
    self.message.get_delta.share = kwargs['volume']
    if kwargs['from_generation'] == -1: # a special marker for us to say "from scratch"
      self.message.get_delta.from_scratch = True
    else:
      self.message.get_delta.from_generation = kwargs['from_generation']


  def delta_info(self, msg):
    """Function called when we receive a DELTA_INFO message."""
    info = msg.delta_info
    # print 'DELTA_INFO (id', msg.id, 'volume:', self.volume, '):', 'generation:', info.generation, 'live:', info.is_live, 'type:', info.type
    info = msg.delta_info.file_info
    # print 'file info:', 'type:', info.type, 'parent:', info.parent, 'share:', info.share, 'node:', info.node, 'name:', info.name, 'is_public:', info.is_public, 'content hash:', info.content_hash, 'crc32:', info.crc32, 'size in bytes:', info.size, 'last modified:', date.fromtimestamp(info.last_modified)
    if self.on_delta_info:
      self.on_delta_info(msg.delta_info)


  def delta_end(self, msg):
    info = msg.delta_end
    # print 'DELTA_END:', 'generation:', info.generation, 'full:', info.full, 'free bytes:', info.free_bytes
    # print ''
    # print ''
    if self.on_delta_end:
      self.on_delta_end(self.volume, info)



class Pong(U1Request):
  """PONG request class.
  A message sent in response to a PING."""

  def __init__(self, handler, **kwargs):
    U1Request.__init__(self, handler, kwargs)
    self.message.type = protocol_pb2.Message.PONG



class GetContent(U1Request):
  """GET_CONTENT request class.
  Used when we need to download a file's content."""

  def __init__(self, handler, **kwargs):
    U1Request.__init__(self, handler, kwargs)
    self.message.type = protocol_pb2.Message.GET_CONTENT
    self.states[protocol_pb2.Message.NODE_ATTR] = self.node_attr
    self.states[protocol_pb2.Message.BYTES] = self.bytes
    self.states[protocol_pb2.Message.EOF] = self.eof
    self.end_msg_type = protocol_pb2.Message.EOF # the type of message signing the end of request
    # fill the message get_content fields with what we got in kwargs
    self.message.get_content.share = kwargs['share']
    self.message.get_content.node = kwargs['node']
    self.message.get_content.hash = kwargs['hash']
#    self.message.get_content.offset = kwargs['offset']
    self.filename = kwargs['filename'] # name of the file to download the content to
    self.downloaded = 0 # Number of compressed bytes downloaded
    self.actual_bytes = 0 # Actual size of deflated data received (useful for offseting)
    self.offset = kwargs['offset'] # Offset of the decompressed data at which we want to start getting the data


  def node_attr(self, msg):
    print 'Received node attr', msg
    volume = self.message.get_content.share
    node = self.message.get_content.node
    self.node_attr = {field[0].name: field[1] for field in msg.node_attr.ListFields()}
    # check transmitted sha1 and crc32
    try:
      if self.handler.volumes[volume][node]['crc32'] != self.node_attr['crc32'] or self.handler.volumes[volume][node]['hash'] != self.node_attr['hash']:
        raise RequestException("Server's file's SHA-1 or CRC32 doesn't match local one")
    except KeyError: # can happen when, asynchronously, the volumes list hasn't been filled yet
      pass
    
  def bytes(self, msg):
    # print 'Received bytes size:', len(msg.bytes.bytes), msg
    data = zlib.decompress(msg.bytes.bytes)
    # print 'zlib:"', data, '"'
    # Cut data we don't want when we reach _the_ message that first matches the required offset
    if self.actual_bytes < self.offset and (self.actual_bytes + len(data) > self.offset):
      # print 'data'
      data = data[(self.offset-self.actual_bytes):]
    # print data
    self.actual_bytes += len(data)
    # print self.actual_bytes, self.offset
    if self.actual_bytes >= self.offset: # if we reach the offset of interest, put the data in the file
      if self.downloaded is 0: # if it is the first chunk we receive
        open_fmt = 'w' # truncate file's content if any
      else:
        open_fmt = 'a' # otherwise just append
      with open(self.filename, open_fmt) as fi:
        fi.write(data)
    self.downloaded += len(msg.bytes.bytes)


  def eof(self, msg):
    # print 'Transfer complete,', self.downloaded, 'bytes downloaded', self.actual_bytes, 'actual bytes'
    # TODO: check self.downloaded == node_attr.deflated_size, and file size == node_attr.size, and re-calculate sha1 and crc32.
    # raise an error if something's wrong.
    pass


class PutContent(U1Request):
  """PUT_CONTENT request class.
  Used when we need to upload a file to the server."""

  def __init__(self, handler, **kwargs):
    U1Request.__init__(self, handler, kwargs)
    self.message.type = protocol_pb2.Message.PUT_CONTENT
    self.states[protocol_pb2.Message.BEGIN_CONTENT] = self.begin_content
    self.states[protocol_pb2.Message.OK] = self.ok
    self.end_msg_type = protocol_pb2.Message.OK # the type of message signing the end of request
    self.message


def msg_type(msg):
  """Helper function.
  Returns the string representation of the message id's protobuf enum name."""
  return protocol_pb2.Message.DESCRIPTOR.enum_types_by_name['MessageType'].values_by_number[msg.type].name
