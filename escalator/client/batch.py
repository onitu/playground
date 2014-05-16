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
