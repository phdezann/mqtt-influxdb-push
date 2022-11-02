import logging
import threading
import uuid

import paho.mqtt.client as mqtt

from mqtt.mqtt_client import MqttClient


class MqttSub(MqttClient):
    def __init__(self, monitor, mqtt_server, mqtt_port, topic, qos, callback, sub_group=None, timeout=5):
        super().__init__(monitor, mqtt_server, mqtt_port, self.__to_topic_name(topic, sub_group))
        self.client_uuid = uuid.uuid4()
        self.qos = qos
        self.callback = callback
        self.timeout = timeout
        self.ready_event = threading.Event()

    def get_client_prefix(self):
        return "sub"

    def start(self):
        super().connect()
        self.client.on_message = self.__on_message
        if not self.ready_event.wait(timeout=self.timeout):
            self.close("Could not subscribe to topic")
        else:
            logging.info(f"Client {self.get_client_id()} ready")

    def on_connect(self, _client, __userdata, _flags_dict, reason, _properties):
        super().on_connect(_client, __userdata, _flags_dict, reason, _properties)
        if reason == 0:
            self.__subscribe()

    def __to_topic_name(self, topic, subscription_group):
        if subscription_group:
            return f"$share/{subscription_group}/{topic}"
        return topic

    def __on_message(self, _1, _2, message):
        try:
            payload = str(message.payload, "utf-8")
            self.callback(payload, message.topic)
        except Exception as e:
            self.close(e)
            raise e

    def __subscribe(self):
        (rc, mid) = self.client.subscribe(self.topic, qos=self.qos)
        if not rc == mqtt.MQTT_ERR_SUCCESS:
            self.close("Could not subscribe")
        else:
            self.ready_event.set()
