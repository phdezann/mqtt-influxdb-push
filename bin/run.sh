#!/usr/bin/env bash

set -e -u -x

python3 -m pip install influxdb-client paho-mqtt tcping

python3 main.py \
  --mqtt-hostname="${MQTT_HOSTNAME}" \
  --mqtt-port="${MQTT_PORT}" \
  --influxdb-hostname="${INFLUXDB_HOSTNAME}" \
  --influxdb-token="${INFLUXDB_TOKEN}" "$@"
