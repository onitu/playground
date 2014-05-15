from threading import Thread

import zmq

import protocol


class Worker(Thread):

    def __init__(self, db, uri, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)

        self.context = zmq.Context.instance()
        self.uri = uri
        self.socket = None

        self.db = db

        self.commands = {
            protocol.GET: self.get,
            protocol.EXISTS: self.exists,
            protocol.PUT: self.put,
            protocol.DELETE: self.delete
        }

    def run(self):
        self.socket = self.context.socket(zmq.REP)
        self.socket.connect(self.uri)

        while True:
            cmd, args = protocol.extract_request(self.socket.recv())
            self.socket.send(self.handle_cmd(cmd, args))

    def handle_cmd(self, cmd, args):
        cb = self.commands.get(cmd)
        if cb:
            try:
                resp = cb(*args)
            except TypeError:
                print("invalid args")
                resp = protocol.format_response(cmd, status=protocol.STATUS_INVALID_ARGS)
        else:
            print("bad command")
            resp = protocol.format_response(cmd, status=protocol.STATUS_CMD_NOT_FOUND)
        return resp

    def get(self, key):
        value = self.db.get(key)
        if value is None:
            return protocol.format_response(key, status=protocol.STATUS_KEY_NOT_FOUND)
        return protocol.format_response(value)

    def exists(self, key):
        value = self.db.get(key)
        return protocol.format_response(value is not None)

    def put(self, key, value):
        self.db.put(key, value)
        return protocol.format_response()

    def delete(self, key):
        self.db.delete(key)
        return protocol.format_response()
