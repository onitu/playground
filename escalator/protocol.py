import msgpack

GET = b'1'
PUT = b'2'


class Status(type):
    maxcode = 0
    registered_status = {}

    def __new__(cls, name, bases, dct):
        cls.maxcode += 1
        dct['code'] = cls.maxcode
        c = super(Status, cls).__new__(cls, name, bases, dct)
        cls.registered_status[c.code] = c
        return c

    @classmethod
    def get(cls, code):
        return cls.registered_status[code]


def with_metaclass(metacls):
    return metacls('Base', (object,), {})


class STATUS_OK(with_metaclass(Status)):
    exception = None


class STATUS_CMD_NOT_FOUND(with_metaclass(Status)):
    class CommandNotFound(KeyError):
        def __init__(self, cmd):
            KeyError.__init__(self, 'No such command {}'.format(repr(cmd)))
    exception = CommandNotFound


class STATUS_INVALID_ARGS(with_metaclass(Status)):
    class InvalidArguments(TypeError):
        def __init__(self, cmd):
            TypeError.__init__(self, 'Invalid arguments for command {}'.format(repr(cmd)))
    exception = InvalidArguments


class STATUS_KEY_NOT_FOUND(with_metaclass(Status)):
    class KeyNotFound(KeyError):
        def __init__(self, key):
            KeyError.__init__(self, 'Key {} not found in base'.format(repr(key)))
    exception = KeyNotFound


class STATUS_ERROR(with_metaclass(Status)):
    class Error(Exception):
        def __init__(self):
            Exception.__init__(self, 'An error occurred')
    exception = Error


def pack_msg(*args):
    return msgpack.packb(args)


def unpack_msg(packed):
    return msgpack.unpackb(packed)


def format_request(cmd, *args):
    return pack_msg(cmd, args)


def format_response(*args, **kwargs):
    status = kwargs.get('status', STATUS_OK)
    return pack_msg(status.code, args)


def extract_request(msg):
    cmd, args = unpack_msg(msg)
    return cmd, args


def extract_response(msg):
    status_code, args = unpack_msg(msg)
    status = Status.get(status_code)
    if status.exception is not None:
        raise status.exception(*args)
    return args
