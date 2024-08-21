import boto3
import json


class KinesisConnector():
    def __init__(self, stream_name, region_name):
        self.stream_name = stream_name
        self.region_name = region_name
        self.client = None

    def connect(self):
        self.client = boto3.client('kinesis', region_name=self.region_name)

    def push_message(self, partition_key, message):
        self.client.put_record(
            StreamName=self.stream_name,
            Data=json.dumps(message),
            PartitionKey=partition_key
        )

    def close(self):
        pass
