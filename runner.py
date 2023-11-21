from queues.python_queue import PythonQueue
from workers.worker import Worker


class Runner:
    def __init__(self, worker_count):
        self.worker_count = worker_count
        self.queues = {"default": PythonQueue()}
        self.workers = {}

    def start_workers(self, queue_name="default", worker_count=None):
        if worker_count is None:
            worker_count = self.worker_count
        if queue_name not in self.queues:
            raise ValueError(f"Queue '{queue_name}' does not exist.")

        self.workers[queue_name] = []
        for _ in range(worker_count):
            worker = Worker(self.queues[queue_name], None)
            worker.daemon = True
            worker.start()
            self.workers[queue_name].append(worker)

    def add_job(self, job, queue_name="default"):
        self.queues[queue_name].put(job)

    def stop_workers(self):
        for queue in self.queues.values():
            for _ in range(self.worker_count):
                queue.put(None)

        for worker_list in self.workers.values():
            for worker in worker_list:
                worker.join()

    def run(self):
        for queue_name in self.queues:
            self.start_workers(queue_name)

        for queue in self.queues.values():
            queue.join()

        self.stop_workers()
