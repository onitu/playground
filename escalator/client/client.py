from threading import Lock

import zmq

import protocol


class WriteBatch(object):
    def __init__(self, db, transaction):
        self.db = db
        self.transaction = transaction
        self.requests = []

    def write(self):
        self.requests.insert(0, protocol.format_request(protocol.BATCH,
                                                        self.db.db_uid,
                                                        self.transaction))
        self.db.socket.send_multipart(self.requests)
        protocol.extract_response(self.db.socket.recv())
        self.requests = []

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not self.transaction or not type:
            self.write()

    def _request(self, cmd, *args):
        self.requests.append(protocol.format_request(cmd, None, *args))

    def put(self, key, value):
        self._request(protocol.PUT, key, value)

    def delete(self, key):
        self._request(protocol.DELETE, key)


class Escalator(object):
    def __init__(self, server='localhost', port=4224, protocol='tcp',
                 addr=None):
        super(Escalator, self).__init__()
        self.db_uid = None
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.lock = Lock()
        if addr is None:
            addr = '{}://{}:{}'.format(protocol, server, port)
        self.socket.connect(addr)

    def _request(self, cmd, *args):
        with self.lock:
            self.socket.send(protocol.format_request(cmd, self.db_uid, *args))
            return protocol.extract_response(self.socket.recv())

    def _request_multi(self, cmd, *args):
        with self.lock:
            self.socket.send(protocol.format_request(cmd, self.db_uid, *args))
            protocol.extract_response(self.socket.recv())
            l = []
            while self.socket.get(zmq.RCVMORE):
                l.append(protocol.unpack_msg(self.socket.recv()))
            return l

    def get(self, key):
        return self._request(protocol.GET, key)[0]

    def exists(self, key):
        return self._request(protocol.EXISTS, key)[0]

    def get_default(self, key, default=None):
        try:
            return self._request(protocol.GET, key)[0]
        except protocol.KeyNotFound:
            return default

    def put(self, key, value):
        self._request(protocol.PUT, key, value)

    def delete(self, key):
        self._request(protocol.DELETE, key)

    def range(self,
              prefix=None, start=None, stop=None,
              include_start=True, include_stop=False,
              include_key=True, include_value=True,
              reverse=False):
        return self._request_multi(protocol.RANGE,
                                   prefix, start, stop,
                                   include_start, include_stop,
                                   include_key, include_value,
                                   reverse)

    def write_batch(self, transaction=False):
        return WriteBatch(self, transaction)

if __name__ == '__main__':
    from multiprocessing.pool import ThreadPool

    k = 10000

    def foo(w):
        w += 1
        client = Escalator()
        for i in range(k):
            client.put(str(w * k + i), str(i))

        for i in range(k):
            print(client.get(str(w * k + i)))

    pool = ThreadPool()
    pool.map(foo, range(5))
