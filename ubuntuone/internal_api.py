import socket, json, protocol_pb2, struct, oauthlib.oauth1, ssl 
from urlparse import urlparse, parse_qsl
from oauthlib.oauth1 import Client, SIGNATURE_PLAINTEXT, SIGNATURE_TYPE_QUERY

def credentials_from_file(credsfile):
    """Extracts the OAuth credentials from file"""
    with open(credsfile) as f:
        jsoncreds = json.loads(f.read())
    return jsoncreds

def recv_message(sock):
    """sad and ugly way to receive a message, and nothing more (temporary !!)"""
    received = 0
    data = ''
    sizeLen = struct.calcsize("!I")
    while received < sizeLen:
        data += sock.read(sizeLen-received)
        received += len(data)
    msgLength = struct.unpack("!I", data[:sizeLen])[0]
    data = data[sizeLen:]
    received -= sizeLen
    while received < msgLength:
        data += sock.read(msgLength-received)
        received += len(data)
    rep = protocol_pb2.Message()
    rep.ParseFromString(data)
    return rep

def send_protobuf(sock, pb):
    pb = pb.SerializeToString()
    # send the message length (according to protocol)
    msgLength = struct.pack("!I", len(pb))
    sock.sendall(msgLength)
    # send the actual message
    sock.sendall(pb)
    

def send_message(sock, id, type, oauth_creds=None):
    message = protocol_pb2.Message()
    message.id = id
    message.type = type
    # Signs the message with oauth (only at authentication time)
    if oauth_creds is not None:
        client = oauthlib.oauth1.Client(oauth_creds["consumer_key"], oauth_creds["consumer_secret"],
                                        oauth_creds["token"], oauth_creds["token_secret"],
                                        signature_method=SIGNATURE_PLAINTEXT,
                                        signature_type=SIGNATURE_TYPE_QUERY)
        url, headers, body = client.sign('http://server')
    # Parse out the authentication parameters from the query string.
        auth_parameters = dict((name, value) for name, value in
                               parse_qsl(urlparse(url).query)
                               if name.startswith('oauth_'))
    # add the authentication informations
        for key, value in auth_parameters.items():
            param = message.auth_parameters.add()
            param.name = key
            param.value = value
    send_protobuf(sock, message)

def msg_type(msg):
    return protocol_pb2.Message.DESCRIPTOR.enum_types_by_name['MessageType'].values_by_number[msg.type].name

def authenticate(sock):
    oauth_creds = credentials_from_file(".credentials")
    send_message(sock, 1, protocol_pb2.Message.AUTH_REQUEST, oauth_creds)
    rep = recv_message(sock)
    print 'rep type:', msg_type(rep), 'number', rep.id
    rep = recv_message(sock)
    print 'rep type:', msg_type(rep), 'number', rep.id, "root:", rep.root.node, 'generation:', rep.root.generation, 'free space:', rep.root.free_bytes    

def ping(sock):
    send_message(sock, 3, protocol_pb2.Message.PING)
    rep = recv_message(sock)
    print 'rep type:', msg_type(rep), 'number', rep.id

def list_volumes(sock):
    send_message(sock, 5, protocol_pb2.Message.LIST_VOLUMES)
    listing = True
    while listing:
        rep = recv_message(sock)
        if rep.type == protocol_pb2.Message.VOLUMES_END:
            print 'end'
            listing = False
        else:
            print 'rep type:', msg_type(rep), 'volume type:', rep.list_volumes.type
            if rep.list_volumes.type == protocol_pb2.Volumes.ROOT:
                print 'root', rep.list_volumes.root.node
            elif rep.list_volumes.type == protocol_pb2.Volumes.UDF:
                print 'udf', rep.list_volumes.udf.volume, rep.list_volumes.udf.node, rep.list_volumes.udf.suggested_path, rep.list_volumes.udf.generation, rep.list_volumes.udf.free_bytes
            elif rep.list_volumes.type == protocol_pb2.Volumes.SHARE:
                print 'share'


def get_delta(sock):
    message = protocol_pb2.Message()
    message.id = 7
    message.type = protocol_pb2.Message.GET_DELTA
#    message.get_delta.from_generation = 409
    message.get_delta.share = '2867b9fd-5cac-4d01-9c72-be2f769b7429'
    message.get_delta.from_scratch = True
    send_protobuf(sock, message)
    listing = True
    files = []
    while listing:
        rep = recv_message(sock)
        if rep.type == protocol_pb2.Message.DELTA_END:
            print 'delta end'
            listing = False
        elif rep.type == protocol_pb2.Message.DELTA_INFO:
            print 'rep type:', msg_type(rep), 'number', rep.id
            print 'generation:', rep.delta_info.generation, 'is_live:', rep.delta_info.is_live, 'type:', rep.delta_info.type
            print 'file infos:'
            print 'type:', rep.delta_info.file_info.type, 'parent:', rep.delta_info.file_info.parent, 'share:', rep.delta_info.file_info.share, 'node:', rep.delta_info.file_info.node, 'name:', rep.delta_info.file_info.name, 'is_public:', rep.delta_info.file_info.is_public, 'content hash:', rep.delta_info.file_info.content_hash, 'crc32:', rep.delta_info.file_info.crc32, 'size:', rep.delta_info.file_info.size, 'last modified:', rep.delta_info.file_info.last_modified
            files.append(rep.delta_info.file_info.name)
        else:
            print 'error'
    print '(', len(files), 'files )', files

def query(sock):
    message = protocol_pb2.Message()
    message.id = 9
    message.type = protocol_pb2.Message.QUERY
    query = message.query.add()
    query.share = '2867b9fd-5cac-4d01-9c72-be2f769b7429' 
    query.node = '372abcb4-5d19-43ba-aa17-005a6aec9714'
    query.hash = 'sha1:6b163fa3bad47a62474afcb81fb9f27bcb549d82'
    send_protobuf(sock, message)
    
    print 'sent'
    rep = recv_message(sock)
    print 'rep type:', msg_type(rep)

def main():
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect(("fs-1.one.ubuntu.com", 443))
    sslSocket = ssl.wrap_socket(conn)
    # read the greeting text
    data = sslSocket.read(4096)
    print data
    authenticate(sslSocket)
    ping(sslSocket)
    list_volumes(sslSocket)
    get_delta(sslSocket)
#    query(sslSocket)
    sslSocket.close()

if __name__ == "__main__":
    main()
