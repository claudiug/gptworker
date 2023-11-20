import random
import threading
import time
from queue import Queue


class Worker(threading.Thread):
    def __init__(self, queue, stop_signal, max_retries=0, retry_delay=1):
        super().__init__()
        self.queue = queue
        self.stop_signal = stop_signal
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def run(self):
        while True:
            job = self.queue.get()
            if job is None:
                break  # Stop signal

            try:
                job.perform()
                job.current_retries = 0  # Reset retry count after successful execution
            except Exception as e:
                job.current_retries += 1
                if job.current_retries <= job.max_retries:
                    print(f"Error: {e}. Retrying... Attempt {job.current_retries} of {job.max_retries}")
                    time.sleep(job.retry_delay)  # Wait before retrying
                    self.queue.put(job)  # Re-queue the job for retry
                else:
                    print(f"Error: {e}. Max retries reached. Not retrying.")
            finally:
                self.queue.task_done()

    @staticmethod
    def perform():
        raise NotImplementedError("This method should be implemented by subclasses.")


class SendMessage(Worker):
    def __init__(self, queue):
        super().__init__(queue, None)
        self.current_retries = 0  # Add retry counter to the job

    def perform(self):
        print("Sending message...")


class MakeUserActive(Worker):
    def __init__(self, queue, max_retries=10, retry_delay=1):
        super().__init__(queue, None, max_retries, retry_delay)
        self.current_retries = 0  # Add retry counter to the job

    def perform(self):
        if random.random() < 0.5:  # 50% chance to simulate an error
            raise Exception("Simulated error: Unable to set user role.")
        print(f"Successfully set role for user")


class Runner:
    def __init__(self, worker_count):
        self.worker_count = worker_count
        self.queue = Queue()
        self.workers = []

    def start_workers(self):
        for _ in range(self.worker_count):
            worker = Worker(self.queue, None)
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def add_job(self, job):
        self.queue.put(job)

    def stop_workers(self):
        for _ in self.workers:
            self.queue.put(None)  # None acts as a stop signal

    def run(self):
        self.start_workers()
        self.queue.join()
        self.stop_workers()


if __name__ == '__main__':
    runner = Runner(worker_count=3)
    runner.add_job(SendMessage(runner.queue))
    runner.add_job(MakeUserActive(runner.queue))
    runner.run()
