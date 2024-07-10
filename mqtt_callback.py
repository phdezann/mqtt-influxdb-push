import json
import re

ELECTRICITY_TOPIC = "tele/+/SENSOR"
GAS_TOPIC = "gas-meter/meter/update"
WEATHER_TOPIC = "weather/+/SENSOR"
SYSINFO_TOPIC = "sysinfo/+/SENSOR"
SANDBOX_TOPIC = "temperature/+/SENSOR"
LIGHT_TOPIC = "light/+/SENSOR"


class MqttCallback:
    def __init__(self, transformer, accumulator, args, influxdb):
        self.transformer = transformer
        self.accumulator = accumulator
        self.args = args
        self.influxdb = influxdb

    def on_message(self, payload, topic):
        probe_id = re.search(r"tele/tasmota_(.+?)/SENSOR", topic, re.IGNORECASE)
        if probe_id:
            identifier = probe_id.group(1)
            payload = json.loads(payload)
            payload = payload['TIC']
            payload = self.transformer.transform_from_mqtt(payload, identifier)
            self.accumulator.put(payload)
            return
        if "gas-meter/meter/update" in topic:
            payload = json.loads(payload)
            payload['owner'] = "67WQW8"
            self.influxdb.push_all([payload], self.args.influxdb_energy_bucket)
            return
        probe_id = re.search(r"weather/probe_(.+?)/SENSOR", topic, re.IGNORECASE)
        if probe_id:
            identifier = probe_id.group(1)
            payload = json.loads(payload)
            payload = self.transformer.add_mandatory_fields(payload, identifier)
            self.influxdb.push_all([payload], self.args.influxdb_weather_bucket)
            return
        probe_id = re.search(r"sysinfo/probe_(.+?)/SENSOR", topic, re.IGNORECASE)
        if probe_id:
            identifier = probe_id.group(1)
            payload = json.loads(payload)
            payload = self.transformer.add_mandatory_fields(payload, identifier)
            self.influxdb.push_all([payload], self.args.influxdb_sysinfo_bucket)
            return
        probe_id = re.search(r"temperature/probe_(.+?)/SENSOR", topic, re.IGNORECASE)
        if probe_id:
            identifier = probe_id.group(1)
            payload = json.loads(payload)
            payload = self.transformer.add_mandatory_fields(payload, identifier)
            self.influxdb.push_all([payload], self.args.influxdb_sandbox_bucket)
            return
        probe_id = re.search(r"light/probe_(.+?)/SENSOR", topic, re.IGNORECASE)
        if probe_id:
            identifier = probe_id.group(1)
            payload = json.loads(payload)
            payload = self.transformer.add_mandatory_fields(payload, identifier)
            self.influxdb.push_all([payload], self.args.influxdb_light_bucket)
            return
