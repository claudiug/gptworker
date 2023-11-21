import time

from workers.worker import Worker


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