import logging
import sys
from argparse import ArgumentParser

import payload_transformer
from accumulator import Accumulator
from buffer import Buffer
from influxdb import Influxdb
from mqtt.mqtt_monitor import MqttClientMonitor, TerminationStatus
from mqtt.mqtt_sub import MqttSub
from mqtt_callback import MqttCallback, WEATHER_TOPIC, ELECTRICITY_TOPIC, GAS_TOPIC, SYSINFO_TOPIC, SANDBOX_TOPIC, LIGHT_TOPIC
from ssh_tunnel import SSHTunnel


def main():
    parser = ArgumentParser()
    parser.add_argument("--influxdb-hostname", default="localhost")
    parser.add_argument("--influxdb-port", default="8086")
    parser.add_argument("--influxdb-org", default="phdezann")
    parser.add_argument("--influxdb-energy-bucket", default="linky")
    parser.add_argument("--influxdb-weather-bucket", default="weather")
    parser.add_argument("--influxdb-sysinfo-bucket", default="sysinfo")
    parser.add_argument("--influxdb-sandbox-bucket", default="sandbox")
    parser.add_argument("--influxdb-light-bucket", default="light")
    parser.add_argument("--influxdb-token")
    parser.add_argument("--buffer-size", type=int, default=10)
    parser.add_argument("--meter-owner")
    parser.add_argument("--mqtt-hostname")
    parser.add_argument("--mqtt-port", type=int)

    args = parser.parse_args()
    ssh_tunnel = SSHTunnel(args)
    accumulator = Accumulator()
    influxdb = Influxdb(args, ssh_tunnel)
    buffer = Buffer(args, accumulator, influxdb)
    transformer = payload_transformer.PayloadTransformer(args)

    mqtt_callback = MqttCallback(transformer, accumulator, args, influxdb)
    monitor = MqttClientMonitor()
    pub_weather = MqttSub(monitor, args.mqtt_hostname, args.mqtt_port, WEATHER_TOPIC, 2, mqtt_callback.on_message)
    pub_electricity = MqttSub(monitor, args.mqtt_hostname, args.mqtt_port, ELECTRICITY_TOPIC, 2, mqtt_callback.on_message)
    pub_gas = MqttSub(monitor, args.mqtt_hostname, args.mqtt_port, GAS_TOPIC, 2, mqtt_callback.on_message)
    pub_sysinfo = MqttSub(monitor, args.mqtt_hostname, args.mqtt_port, SYSINFO_TOPIC, 2, mqtt_callback.on_message)
    pub_sandbox = MqttSub(monitor, args.mqtt_hostname, args.mqtt_port, SANDBOX_TOPIC, 2, mqtt_callback.on_message)
    pub_light = MqttSub(monitor, args.mqtt_hostname, args.mqtt_port, LIGHT_TOPIC, 2, mqtt_callback.on_message)
    pub_weather.start()
    pub_electricity.start()
    pub_gas.start()
    pub_sysinfo.start()
    pub_sandbox.start()
    pub_light.start()
    logging.info("Mqtt publishers are now ready")

    monitor.register_client(buffer)
    buffer.start_reading()

    termination_status = monitor.wait_for_termination()
    monitor.close_all_clients(termination_status)

    if termination_status == TerminationStatus.NORMAL_TERMINATION:
        sys.exit(0)
    elif termination_status == TerminationStatus.ABNORMAL_TERMINATION:
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
    main()
