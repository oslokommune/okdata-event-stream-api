import os

from database import StackTemplate
from util import CONFIDENTIALITY_MAP

ENV = os.environ["ORIGO_ENVIRONMENT"]

pipeline_router_lambda_name = (
    f"pipeline-router-{os.environ['ORIGO_ENVIRONMENT']}-route-kinesis"
)


class EventStreamTemplate:
    def __init__(self, dataset: dict, version: str, updated_by: str, create_raw: bool):
        self.dataset = dataset
        self.version = version
        self.updated_by = updated_by
        self.create_raw = create_raw
        self.shard_count = 1
        self.batch_size = 10
        self.batch_window = 10

    def get_stream_name(self, stage) -> str:
        confidentiality = CONFIDENTIALITY_MAP[self.dataset["accessRights"]]
        return f"dp.{confidentiality}.{self.dataset['Id']}.{stage}.{self.version}.json"

    def generate_stack_template(self) -> StackTemplate:
        resources = {}

        if self.create_raw:
            raw_stream_name = self.get_stream_name(stage="raw")
            resources["RawDataStream"] = self.stream_resource(raw_stream_name)
            resources["RawPipelineTrigger"] = self.pipeline_trigger_resource(
                raw_stream_name, depends_on="RawDataStream"
            )

        processed_stream_name = self.get_stream_name(stage="processed")
        resources["ProcessedDataStream"] = self.stream_resource(processed_stream_name)
        resources["ProcessedPipelineTrigger"] = self.pipeline_trigger_resource(
            processed_stream_name, depends_on="ProcessedDataStream"
        )

        return StackTemplate(
            **{
                "Description": f"Kinesis streams and pipeline triggers for {self.dataset['Id']}/{self.version}",
                "Resources": resources,
            }
        )

    def stream_resource(self, stream_name: str) -> dict:
        return {
            "Type": "AWS::Kinesis::Stream",
            "Properties": {
                "Name": stream_name,
                "ShardCount": self.shard_count,
                "Tags": [{"Key": "created_by", "Value": self.updated_by}],
            },
        }

    def pipeline_trigger_resource(self, stream_name: str, depends_on: str) -> dict:
        return {
            "Type": "AWS::Lambda::EventSourceMapping",
            "Properties": {
                "BatchSize": self.batch_size,
                "Enabled": True,
                "EventSourceArn": {
                    "Fn::Sub": "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream/"
                    + f"{stream_name}"
                },
                "FunctionName": {
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:"
                    + pipeline_router_lambda_name
                },
                "MaximumBatchingWindowInSeconds": self.batch_window,
                "StartingPosition": "LATEST",
            },
            "DependsOn": depends_on,
        }
