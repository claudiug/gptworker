from queues.abstract_queue import AbstractQueue
from queue import Queue as StdQueue


class PythonQueue(AbstractQueue):
    def __init__(self):
        self.queue = StdQueue()

    def get(self):
        return self.queue.get()

    def put(self, item):
        self.queue.put(item)

    def task_done(self):
        self.queue.task_done()

    def join(self):
        self.queue.join()
