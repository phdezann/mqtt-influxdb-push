import logging

from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS


class Influxdb:
    def __init__(self, args, ssh_tunnel):
        self.args = args
        self.ssh_tunnel = ssh_tunnel
        if not self.args.influxdb_hostname == "localhost":
            self.ssh_tunnel.start_tunnel()

    def push_all(self, records, bucket):
        for record in records:
            logging.info(f"Pushing record to influxdb: {record}")
            self.__push(record, bucket)

    def __push(self, record, bucket):
        creation = record.pop('creation')
        owner = record.pop('owner')

        point = Point(bucket) \
            .tag("owner", owner) \
            .time(creation)

        for key, value in record.items():
            point.field(key, value)

        client = InfluxDBClient(url=self.__to_url(self.args), token=self.args.influxdb_token, org=self.args.influxdb_org)
        client \
            .write_api(write_options=SYNCHRONOUS) \
            .write(bucket=bucket, record=point)
        logging.debug(f"Record sent: {point}")

    def __to_url(self, args):
        port = args.influxdb_port
        return f"http://localhost:{port}"
