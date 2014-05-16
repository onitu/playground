from threading import Thread

import zmq

import protocol


class Multipart(list):
    pass


class Worker(Thread):

    def __init__(self, databases, uri, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)

        self.context = zmq.Context.instance()
        self.uri = uri
        self.socket = None

        self.databases = databases

        self.db_commands = {
            protocol.CREATE: self.create,
            protocol.CONNECT: self.connect
        }

        self.commands = {
            protocol.GET: self.get,
            protocol.EXISTS: self.exists,
            protocol.PUT: self.put,
            protocol.DELETE: self.delete,
            protocol.RANGE: self.range,
            protocol.BATCH: self.batch
        }

        self.batch_commands = {
            protocol.PUT: self.batch_put,
            protocol.DELETE: self.batch_delete
        }

    def run(self):
        self.socket = self.context.socket(zmq.REP)
        self.socket.connect(self.uri)

        while True:
            cmd, uid, args = protocol.extract_request(self.socket.recv())
            try:
                if cmd in self.db_commands:
                    db = None
                else:
                    db = self.databases.get(uid)
            except:
                resp = protocol.format_response(
                    uid, status=protocol.STATUS_NO_DB)
            else:
                if db:
                    resp = self.handle_cmd(db, self.commands, cmd, args)
                else:
                    resp = self.handle_cmd(None, self.db_commands, cmd, args)
            if isinstance(resp, Multipart):
                self.socket.send_multipart(resp)
            else:
                self.socket.send(resp)

    def handle_cmd(self, db, commands, cmd, args):
        cb = commands.get(cmd)
        if cb:
            try:
                resp = cb(db, *args) if db is not None else cb(*args)
            except TypeError:
                print("invalid args")
                resp = protocol.format_response(
                    cmd, status=protocol.STATUS_INVALID_ARGS)
        else:
            print("bad command")
            resp = protocol.format_response(
                cmd, status=protocol.STATUS_CMD_NOT_FOUND)
        return resp

    def create(self, name):
        return self.connect(name, True)

    def connect(self, name, create):
        name = name.decode()
        try:
            uid = self.databases.connect(name, create)
            resp = protocol.format_response(uid, status=protocol.STATUS_OK)
        except self.databases.NotExistError as e:
            print('database does not exist:', e)
            resp = protocol.format_response(
                name, status=protocol.STATUS_DB_NOT_FOUND)
        except Exception as e:
            print('database error:', e)
            resp = protocol.format_response(
                name, status=protocol.STATUS_DB_ERROR)
        return resp

    def get(self, db, key):
        value = db.get(key)
        if value is None:
            return protocol.format_response(
                key, status=protocol.STATUS_KEY_NOT_FOUND)
        return protocol.format_response(value)

    def exists(self, db, key):
        value = db.get(key)
        return protocol.format_response(value is not None)

    def put(self, db, key, value):
        db.put(key, value)
        return protocol.format_response()

    def delete(self, db, key):
        db.delete(key)
        return protocol.format_response()

    def range(self, db,
              prefix, start, stop,
              include_start, include_stop,
              include_key, include_value,
              reverse):
        values = Multipart(protocol.pack_arg(v) for v in
                           self.db.iterator(prefix=prefix,
                                            start=start,
                                            stop=stop,
                                            include_start=include_start,
                                            include_stop=include_stop,
                                            include_key=include_key,
                                            include_value=include_value,
                                            reverse=reverse))
        values.insert(0, protocol.format_response())
        return values

    def batch(self, db, transaction):
        with self.db.write_batch(transaction=transaction) as wb:
            while self.socket.get(zmq.RCVMORE):
                cmd, _, args = protocol.extract_request(self.socket.recv())
                self.handle_cmd(wb, self.batch_commands, cmd, args)
        return protocol.format_response()

    def batch_put(self, wb, key, value):
        wb.put(key, value)

    def batch_delete(self, wb, key):
        wb.delete(key)
