from abc import ABC, abstractmethod


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
