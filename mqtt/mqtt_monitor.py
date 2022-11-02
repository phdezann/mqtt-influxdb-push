from enum import Enum, auto
from threading import Event


class TerminationStatus(Enum):
    NORMAL_TERMINATION = auto()
    ABNORMAL_TERMINATION = auto()


class MqttClientMonitor:
    def __init__(self):
        self.clients = []
        self.exit = Event()

    def register_client(self, client):
        self.clients.append(client)

    def terminate_all(self):
        self.exit.set()

    def wait_for_termination(self):
        while True:
            if not self.__all_clients_active():
                return TerminationStatus.ABNORMAL_TERMINATION
            if self.exit.wait(1):
                return TerminationStatus.NORMAL_TERMINATION

    def close_all_clients(self, status):
        for client in self.clients:
            client.close(f"Application shutdown: {status.name}")

    def __all_clients_active(self):
        for client in self.clients:
            if not client.is_active():
                return False
        return True
