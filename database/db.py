import os
import boto3
import json
from database import EventStream
from boto3.dynamodb.conditions import Key


class EventStreamsTable:
    def __init__(self):
        table_name = "event-streams"
        dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
        self.table = dynamodb.Table(table_name)

    def put_event_stream(self, event_stream: EventStream, event_stream_version):
        event_stream_item = json.loads(event_stream.json())
        event_stream_item["event_stream_version"] = event_stream_version
        self.table.put_item(Item=event_stream_item)

    def get_event_stream(self, event_stream_id):
        event_stream_items = self.table.query(
            IndexName="by_id", KeyConditionExpression=Key("id").eq(event_stream_id)
        )["Items"]
        if event_stream_items:
            current_item = max(
                event_stream_items, key=lambda item: item["event_stream_version"]
            )
            return EventStream(**current_item)
