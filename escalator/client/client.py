import zmq


class Escalator(object):

    def __init__(self, *args, **kwargs):
        super(Escalator, self).__init__(*args, **kwargs)

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect('tcp://127.0.0.1:4224')

    def _send(self, *data):
        self.socket.send_multipart(data)
        return self.socket.recv()

    def get(self, key):
        return self._send('1', key)

    def put(self, key, value):
        return self._send('2', key, value)

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
