import zmq

import protocol


class Escalator(object):

    def __init__(self, server='localhost', port=4224, protocol='tcp',
                 addr=None):
        super(Escalator, self).__init__()

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        if addr is None:
            addr = '{}://{}:{}'.format(protocol, server, port)
        self.socket.connect(addr)

    def _request(self, *args):
        self.socket.send(protocol.format_request(*args))
        return protocol.extract_response(self.socket.recv())

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
