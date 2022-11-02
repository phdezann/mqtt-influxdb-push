import logging
import threading
import uuid

import paho.mqtt.client as mqtt


class MqttClient:
    def __init__(self, monitor, mqtt_server, mqtt_port, topic):
        monitor.register_client(self)
        self.mqtt_server = mqtt_server
        self.mqtt_port = mqtt_port
        self.topic = topic
        self.active = True
        self.loop_thread = None
        self.client_uuid = uuid.uuid4()
        self.client = self.__new_client()

    def __new_client(self):
        client = mqtt.Client(client_id=f"{self.get_client_id()}", protocol=mqtt.MQTTv5)
        client.on_connect = self.on_connect
        if self.mqtt_port == 8883:
            client.tls_set()
        return client

    def get_client_id(self):
        return f"{self.get_client_prefix()}-{self.client_uuid}-{self.topic}"

    def get_client_prefix(self):
        raise Exception("Not implemented")

    def connect(self):
        self.loop_thread = threading.Thread(target=self.client.loop_forever, args=())
        self.loop_thread.start()
        try:
            self.client.connect(self.mqtt_server, port=self.mqtt_port)
        except Exception as e:
            self.close(e)

    def on_connect(self, _client, __userdata, _flags_dict, reason, _properties):
        if not reason == 0:
            self.close(f"Could not connect, reason was {reason.getName()}")

    def get_loop_thread(self):
        return self.loop_thread

    def is_active(self):
        return self.active

    def close(self, err_msg):
        if self.active:
            self.client.disconnect()
            logging.warning(f"Client '{self.get_client_id()}' closed due to '{err_msg}'")
            self.active = False
