import zmq
import plyvel

from .worker import Worker

context = zmq.Context()

back_uri = 'tcp://127.0.0.1:4225'

proxy = zmq.devices.ProcessDevice(
    device_type=zmq.QUEUE, in_type=zmq.DEALER, out_type=zmq.ROUTER
)
proxy.bind_out('tcp://*:4224')
proxy.bind_in(back_uri)
proxy.start()

db = plyvel.DB('dbs/default', create_if_missing=True)

nb_workers = 8
workers = []

for i in range(nb_workers):
    worker = Worker(db, back_uri)
    worker.start()
    workers.append(worker)

try:
    proxy.join()
except KeyboardInterrupt:
    print("Stopping...")
    pass
