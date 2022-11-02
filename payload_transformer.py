import datetime


class PayloadTransformer:
    def __init__(self, args):
        self.args = args

    def transform_from_mqtt(self, payload, sensor_id):
        owner = self.args.meter_owner
        if owner:
            raise ValueError("Argument owner is set but shouldn't, please remove --meter-owner")
        return self.transform(payload, sensor_id)

    def transform(self, payload, owner):
        self.divide_value_by_1000(payload, 'BASE')
        self.divide_value_by_1000(payload, 'HCHC')
        self.divide_value_by_1000(payload, 'HCHP')
        del payload['ADCO']
        return self.add_mandatory_fields(payload, owner)

    def add_mandatory_fields(self, payload, owner):
        payload['creation'] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        payload['owner'] = owner
        return payload

    def divide_value_by_1000(self, payload, key):
        if key in payload.keys():
            payload[key] = payload[key] / 1000
