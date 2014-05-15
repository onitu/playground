import msgpack

_registered_commands = {}


def command(name, value):
    if value in _registered_commands:
        raise ValueError('Command {} already exists'.format(value))
    _registered_commands[value] = name
    return value


def get_command(value):
    return _registered_commands[value]


GET = command('GET', b'\x01')
EXISTS = command('EXISTS', b'\x02')
PUT = command('PUT', b'\x03')
DELETE = command('DELETE', b'\x04')
RANGE = command('RANGE', b'\x05')


class Status(type):
    _registered_status = []

    def __new__(cls, name, bases, dct):
        dct['code'] = len(cls._registered_status)
        c = super(Status, cls).__new__(cls, name, bases, dct)
        cls._registered_status.append(c)
        return c

    @classmethod
    def get(cls, code):
        return cls._registered_status[code]


def new_status(name, exception):
    return Status(name, (object,), {'exception': exception})


STATUS_OK = new_status('STATUS_OK', None)


class CommandNotFound(KeyError):
    def __init__(self, cmd):
        KeyError.__init__(self, 'No such command {}'.format(repr(cmd)))
STATUS_CMD_NOT_FOUND = new_status('STATUS_CMD_NOT_FOUND', CommandNotFound)


class InvalidArguments(TypeError):
    def __init__(self, cmd):
        TypeError.__init__(self, 'Invalid arguments for command {}'.
                           format(get_command(cmd)))
STATUS_INVALID_ARGS = new_status('STATUS_INVALID_ARGS', InvalidArguments)


class KeyNotFound(KeyError):
    def __init__(self, key):
        KeyError.__init__(self, 'Key {} not found in base'.format(repr(key)))
STATUS_KEY_NOT_FOUND = new_status('STATUS_KEY_NOT_FOUND', KeyNotFound)


class Error(Exception):
    def __init__(self):
        Exception.__init__(self, 'An error occurred')
STATUS_ERROR = new_status('STATUS_ERROR', Error)


def pack_arg(arg):
    return msgpack.packb(arg)


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
