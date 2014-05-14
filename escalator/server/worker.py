from threading import Thread

import zmq


class Worker(Thread):

    def __init__(self, db, uri, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)

        self.context = zmq.Context.instance()
        self.uri = uri
        self.socket = None

        self.db = db

        self.commands = {
            '1': self.get,
            '2': self.put
        }

    def run(self):
        self.socket = self.context.socket(zmq.REP)
        self.socket.connect(self.uri)

        while True:
            cmd = self.socket.recv()
            cb = self.commands.get(cmd)

            if not cb:
                # send error
                print("bad command")
                self.socket.send(None)
                return

            resp = cb()

            if self.socket.get(zmq.RCVMORE):
                # send error
                print("Too much data sent")
                self.socket.send(None)
                return

            self.socket.send(resp)

    def get(self):
        key = self.socket.recv()
        return self.db.get(key)

    def put(self):
        key = self.socket.recv()
        value = self.socket.recv()
        self.db.put(key, value)
        return '1'
