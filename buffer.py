import threading
import logging


class Buffer:
    def __init__(self, args, accumulator, influxdb):
        self.args = args
        self.buffer = []
        self.accumulator = accumulator
        self.influxdb = influxdb
        self.active = True

    def is_active(self):
        return self.active

    def close(self, err_msg):
        if self.active:
            logging.warning(f"Buffer closed due to '{err_msg}'")
            self.active = False

    def read(self):
        try:
            while self.active:
                result = self.accumulator.get()
                if result is None:
                    continue
                if not self.args.influxdb_hostname == "localhost":
                    if len(self.buffer) >= self.args.buffer_size:
                        buffer_copy = self.__copy_and_empty_buffer()
                        self.influxdb.push_all(buffer_copy, self.args.influxdb_energy_bucket)
                    else:
                        self.buffer.append(result)
                else:
                    self.influxdb.push_all([result], self.args.influxdb_energy_bucket)
        except Exception as e:
            self.active = False
            raise e

    def __copy_and_empty_buffer(self):
        copy = self.buffer.copy()
        self.buffer.clear()
        return copy

    def start_reading(self):
        thread = threading.Thread(target=self.read, args=())
        thread.start()
        return thread
