import time

from workers.worker import Worker


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