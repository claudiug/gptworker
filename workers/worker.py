import logging
import threading
import time

from queues.abstract_queue import AbstractQueue


class Worker(threading.Thread):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    def __init__(self, queue: AbstractQueue, stop_signal, max_retries=0, retry_delay=1):
        super().__init__()
        self.queue = queue
        self.stop_signal = stop_signal
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        while True:
            job = self.queue.get()
            if job is self.stop_signal:
                self.logger.info(f"Job execution stop  Job ID: {self}.")
                break

            try:
                self.logger.info(f"Job execution start  Job ID: {self}, Type: {job}")
                job.perform()
                self.logger.info(f"Job execution completed  Job ID: {self}.")
                job.current_retries = 0
            except Exception as e:
                job.current_retries += 1
                self.logger.info(f"Job execution retry  Job ID: {job}, Type: {self}")
                if job.current_retries <= job.max_retries:
                    self.logger.warning(f"Error: {e}. Retrying... Attempt {job.current_retries} of {job.max_retries}")
                    time.sleep(job.retry_delay)
                    self.queue.put(job)
                else:
                    self.logger.error(f"Error: {e}. Max retries reached for Job ID: {job}.")
            finally:
                self.logger.info(f"Job execution done  Job ID: {job}.")
                self.queue.task_done()

    @staticmethod
    def perform():
        raise NotImplementedError("This method should be implemented by subclasses.")
