from queue import Queue, Empty


class Accumulator:

    def __init__(self):
        self.queue = Queue()

    def put(self, v):
        self.queue.put(v, block=False)

    def get(self):
        try:
            return self.queue.get(block=True, timeout=1)
        except Empty:
            return None
