import random
import time

from workers.worker import Worker


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