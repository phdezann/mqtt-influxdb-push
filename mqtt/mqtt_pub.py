import logging
import threading
import uuid

import paho.mqtt.client as mqtt

from mqtt.mqtt_client import MqttClient


class MqttPub(MqttClient):
    def __init__(self, monitor, mqtt_server, mqtt_port, topic, qos, timeout=5):
        super().__init__(monitor, mqtt_server, mqtt_port, topic)
        self.client_uuid = uuid.uuid4()
        self.qos = qos
        self.timeout = timeout
        self.ready_event = threading.Event()

    def get_client_prefix(self):
        return "pub"

    def start(self):
        super().connect()
        if not self.ready_event.wait(timeout=self.timeout):
            self.close("Could not connect")
        else:
            logging.info(f"Client {self.get_client_id()} ready")

    def on_connect(self, _client, __userdata, _flags_dict, reason, _properties):
        super().on_connect(_client, __userdata, _flags_dict, reason, _properties)
        if reason == 0:
            self.ready_event.set()

    def publish(self, message):
        rc, _ = self.client.publish(self.topic, message, qos=self.qos)
        if not rc == mqtt.MQTT_ERR_SUCCESS:
            error_msg = "Could not publish message"
            self.close(error_msg)
            raise Exception(error_msg)
