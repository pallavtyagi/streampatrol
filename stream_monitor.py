from kafka_connector import KafkaConnector
from pyspark.sql.streaming import StreamingQueryListener
import json


class CustomStreamingQueryListener(StreamingQueryListener):
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        super().__init__()
        self.kafka_connector = KafkaConnector(kafka_bootstrap_servers)
        self.kafka_topic = kafka_topic

    def onQueryStarted(self, event):
        event_data = {
            "event": "QueryStarted",
            "id": str(event.id),
            "name": event.name,
            "timestamp": event.timestamp
        }
        self.kafka_connector.push_message(self.kafka_topic, event_data)
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        progress = event.progress
        event_data = {
            "event": "QueryProgress",
            "id": str(progress.id),
            "runId": str(progress.runId),
            "name": progress.name,
            "timestamp": progress.timestamp,
            "batchId": progress.batchId,
            "numInputRows": progress.numInputRows,
            "inputRowsPerSecond": progress.inputRowsPerSecond,
            "processedRowsPerSecond": progress.processedRowsPerSecond,
            "durationMs": progress.durationMs,
            "stateOperators": [op.json for op in progress.stateOperators],
            "sources": [source.json for source in progress.sources],
            "sink": progress.sink.json
        }
        self.kafka_connector.push_message(self.kafka_topic, event_data)
        print(f"Query made progress: {event.progress}")

    def onQueryTerminated(self, event):
        event_data = {
            "event": "QueryTerminated",
            "id": str(event.id),
            "runId": str(event.runId),
            "exception": event.exception
        }
        self.kafka_connector.push_message(self.kafka_topic, event_data)
        print(f"Query terminated: {event.id}")

    def onQueryIdle(self, event):
        event_data = {
            "event": "QueryIdle",
            "id": str(event.id),
            "runId": str(event.runId),
            "timestamp": event.timestamp
        }
        self.kafka_connector.push_message(self.kafka_topic, event_data)
        print(f"Query is idle: {event.id}")

    def __del__(self):
        self.kafka_connector.close()
