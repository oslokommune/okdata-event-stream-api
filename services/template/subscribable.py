import os

from database import StackTemplate
from util import CONFIDENTIALITY_MAP

ENV = os.environ["OKDATA_ENVIRONMENT"]


class SubscribableTemplate:
    def __init__(self, dataset: dict, version: str):
        self.dataset = dataset
        self.version = version
        self.batch_size = 10

    def get_stream_name(self):
        confidentiality = CONFIDENTIALITY_MAP[self.dataset["accessRights"]]
        return (
            f"dp.{confidentiality}.{self.dataset['Id']}.processed.{self.version}.json"
        )

    def get_event_publisher_lambda(self):
        return f"event-data-subscription-{ENV}-publish_event"

    def generate_stack_template(self):
        resources = {}
        resources["SubscriptionSource"] = self.subscription_source()
        return StackTemplate(
            **{
                "Description": f"Subscription event source mapping for {self.dataset['Id']}/{self.version}",
                "Resources": resources,
            }
        )

    def subscription_source(self):
        return {
            "Type": "AWS::Lambda::EventSourceMapping",
            "Properties": {
                "BatchSize": self.batch_size,
                "Enabled": True,
                "EventSourceArn": {
                    "Fn::Sub": "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream/"
                    + self.get_stream_name()
                },
                "FunctionName": {
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:"
                    + self.get_event_publisher_lambda()
                },
                "StartingPosition": "LATEST",
            },
        }
