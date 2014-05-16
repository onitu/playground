import msgpack

import protocol


def pack_arg(arg):
    return msgpack.packb(arg)


def pack_msg(*args):
    return msgpack.packb(args)


def unpack_msg(packed):
    return msgpack.unpackb(packed)


def format_request(cmd, uid, *args):
    return pack_msg(cmd, uid, args)


def format_response(*args, **kwargs):
    status = kwargs.get('status', protocol.status.OK)
    return pack_msg(status.code, args)


def extract_request(msg):
    cmd, uid, args = unpack_msg(msg)
    return cmd, uid, args


def extract_response(msg):
    status_code, args = unpack_msg(msg)
    status = protocol.status.Status.get(status_code)
    if status.exception is not None:
        raise status.exception(*args)
    return args
