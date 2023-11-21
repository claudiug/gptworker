import logging
from abc import ABC, abstractmethod
from queue import Queue as StdQueue
import random
import threading
import time


class AbstractQueue(ABC):
    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def put(self, item):
        pass

    @abstractmethod
    def task_done(self):
        pass


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


class MailQueue(AbstractQueue):
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


class Worker(threading.Thread):
    logging.basicConfig(
        level=logging.DEBUG,  # Set the log level to DEBUG for more detailed logs
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log format
    )

    def __init__(self, queue: AbstractQueue, stop_signal, max_retries=0, retry_delay=1):
        super().__init__()
        self.queue = queue
        self.stop_signal = stop_signal
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.job_id = None  # Variable to store the current job's ID
        self.logger = logging.getLogger(self.__class__.__name__)  # Create a logger for the worker class

    def run(self):
        while True:
            job = self.queue.get()
            if job is self.stop_signal:
                self.logger.info(f"Job execution stop  Job ID: {self}.")
                break  # Stop signal

            try:
                self.logger.info(f"Job execution start  Job ID: {self}, Type: {job}")
                job.perform()
                self.logger.info(f"Job execution completed  Job ID: {self}.")
                job.current_retries = 0  # Reset retry count after successful execution
            except Exception as e:
                job.current_retries += 1
                self.logger.info(f"Job execution retry  Job ID: {job}, Type: {self}")
                if job.current_retries <= job.max_retries:
                    self.logger.warning(f"Error: {e}. Retrying... Attempt {job.current_retries} of {job.max_retries}")
                    time.sleep(job.retry_delay)  # Wait before retrying
                    self.queue.put(job)  # Re-queue the job for retry
                else:
                    self.logger.error(f"Error: {e}. Max retries reached for Job ID: {job}.")
            finally:
                self.logger.info(f"Job execution done  Job ID: {job}.")
                self.queue.task_done()

    @staticmethod
    def perform():
        raise NotImplementedError("This method should be implemented by subclasses.")


class SendMessage(Worker):
    def __init__(self, queue, delay=0):
        super().__init__(queue, None)
        self.delay = delay
        self.current_retries = 0  # Add retry counter to the job

    def perform(self):
        if self.delay > 0:
            print(f"Delaying execution by {self.delay} seconds...")
            time.sleep(self.delay)
            print("Sending message...")
        print("Sending message...")


class MakeUserActive(Worker):
    def __init__(self, queue, max_retries=10, retry_delay=1, delay=1):
        super().__init__(queue, None, max_retries, retry_delay)
        self.delay = delay
        self.current_retries = 0  # Add retry counter to the job

    def perform(self):
        if self.delay > 0:
            print(f"Delaying execution by {self.delay} seconds...")
            time.sleep(self.delay)
        if random.random() < 0.5:  # 50% chance to simulate an error
            raise Exception("Simulated error: Unable to set user role.")
        print(f"Successfully set role for user")


class GenerateImages(Worker):
    def __init__(self, queue, max_retries=10, retry_delay=1, delay=3):
        super().__init__(queue, None, max_retries, retry_delay)
        self.delay = delay

    def perform(self):
        if self.delay > 0:
            print(f"Delaying execution by {self.delay} seconds...")
            time.sleep(self.delay)
            # Implement the logic for generating images
        print("Generating images...")


class Runner:
    def __init__(self, worker_count):
        self.worker_count = worker_count
        self.queues = {"default": PythonQueue(), "image_generation": PythonQueue()}
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
                self.add_job(None)  # Add None as a stop signal to the queue

        for worker_list in self.workers.values():
            for worker in worker_list:
                worker.join()  # Call join on worker threads, not queues

    def run(self):
        for queue_name in self.queues:
            self.start_workers(queue_name)

        for queue in self.queues.values():
            queue.join()  # Wait for all jobs in the queue to be processed

        self.stop_workers()  # Stop the worker threads


if __name__ == '__main__':
    runner = Runner(worker_count=10)
    runner.queues["mailing"] = MailQueue()
    runner.add_job(SendMessage(runner.queues["default"]))
    runner.add_job(MakeUserActive(runner.queues["default"]))
    runner.add_job(GenerateImages(runner.queues["image_generation"]), queue_name="image_generation")
    # Run the system
    runner.run()
