import os
from database import EventStream, Sink, SinkType, StackTemplate

ENV = os.environ["ORIGO_ENVIRONMENT"]


class EventStreamSinkTemplate:
    def __init__(
        self, event_stream: EventStream, dataset: dict, version: str, sink: Sink
    ):
        self.event_stream = event_stream
        self.dataset = dataset
        self.version = version
        self.sink = sink

    def generate_stack_template(self) -> StackTemplate:
        resources = {}
        if self.sink.type == SinkType.S3.value:
            resources = self.firehose_s3()
        elif self.sink.type == SinkType.ELASTICSEARCH.value:
            resources = self.firehose_elasticsearch()
        else:
            raise Exception("No such sink available!")

        return StackTemplate(
            **{
                "Description": f"Firehose for {self.event_stream.id}: {self.sink.type}",
                "Resources": resources,
            }
        )

    def firehose_s3(self) -> dict:
        kinesis_source_stream_name = f"dp.{self.dataset['confidentiality']}.{self.dataset['Id']}.processed.{self.version}.json"
        kinesis_source_base_arn = (
            "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream"
        )
        # Where are we reading data from:
        kinesis_source_arn = f"{kinesis_source_base_arn}/{kinesis_source_stream_name}"

        delivery_stream_name = f"{kinesis_source_stream_name}-sink-{self.sink.type}"

        output_bucket_arn = f"arn:aws:s3:::ok-origo-dataplatform-event-sink-{ENV}"
        output_prefix = (
            f"{self.dataset['confidentiality']}/{self.dataset['Id']}/{self.version}/"
        )

        # We use one shared role for both kinesis source & s3 destination
        # This role holds everything to read & write to the correct resources
        s3_sink_role = "arn:aws:iam::${AWS::AccountId}:role/event_stream_sink_s3_role"

        properties = {
            "DeliveryStreamName": delivery_stream_name,
            "DeliveryStreamType": "KinesisStreamAsSource",
            "KinesisStreamSourceConfiguration": {
                "KinesisStreamARN": {"Fn::Sub": kinesis_source_arn},
                "RoleARN": {"Fn::Sub": s3_sink_role},
            },
            "S3DestinationConfiguration": {
                "BucketARN": output_bucket_arn,
                "BufferingHints": {"IntervalInSeconds": 60, "SizeInMBs": 1},
                "ErrorOutputPrefix": "error/",
                "Prefix": output_prefix,
                "RoleARN": {"Fn::Sub": s3_sink_role},
            },
        }
        """
        S3DestinationConfiguration also have the following (optional) options:
            "CloudWatchLoggingOptions" : CloudWatchLoggingOptions,
            "CompressionFormat" : String,
            "EncryptionConfiguration" : EncryptionConfiguration,
        """

        return {
            "SinkS3Resource": {
                "Type": "AWS::KinesisFirehose::DeliveryStream",
                "Properties": properties,
            }
        }

    def firehose_elasticsearch(self):
        properties = {}
        return {
            "SinkS3Resource": {
                "Type": "AWS::KinesisFirehose::DeliveryStream",
                "Properties": properties,
            }
        }
