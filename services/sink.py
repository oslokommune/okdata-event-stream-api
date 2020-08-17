import os
from origo.data.dataset import Dataset
from database import EventStreamsTable, EventStream, Sink, SinkType, StackTemplate
from services import (
    ResourceNotFound,
    SubResourceNotFound,
    datetime_utils,
)


ENV = os.environ["ORIGO_ENVIRONMENT"]

# Do not expose all internal information about a sink, just the ones
# needed for the owner of the sink
API_FIELDS_SINK = {"id": "id", "type": "type", "cf_status": "status"}


def sink_for_api(sink: dict) -> dict:
    ret = {}
    for key in API_FIELDS_SINK:
        ret[API_FIELDS_SINK[key]] = getattr(sink, key)
    return ret


class EventStreamSinkService:
    def __init__(self, dataset_client: Dataset):
        self.event_streams_table = EventStreamsTable()

    def get_event_stream(self, dataset_id: str, version: str):
        event_stream_id = f"{dataset_id}/{version}"
        return self.event_streams_table.get_event_stream(event_stream_id)

    def get_sinks(self, dataset_id, version) -> list:
        event_stream = self.get_event_stream(dataset_id, version)
        if not event_stream.sinks:
            return []
        return event_stream.sinks

    def get_sinks_for_api(self, dataset_id: str, version: str) -> list:
        sinks = self.get_sinks(dataset_id, version)
        sink_list = []
        for sink in sinks:
            if sink.deleted:
                continue
            sink_list.append(sink_for_api(sink))
        return sink_list

    def get_sink(self, dataset_id: str, version: str, sink_id: str) -> dict:
        sinks = self.get_sinks(dataset_id, version)
        for sink in sinks:
            if sink.id == sink_id and sink.deleted:
                raise SubResourceNotFound
            elif sink.id == sink_id:
                return sink
        raise SubResourceNotFound

    def get_sink_for_api(self, dataset_id: str, version: str, sink_id: str) -> dict:
        existing_sink = self.get_sink(dataset_id, version, sink_id)
        return sink_for_api(existing_sink)

    def add_sink(self, event_stream, dataset_id, version, sink, updated_by):
        dataset = self.dataset_client.get_dataset(dataset_id)
        sink_name = f"event-sink-{dataset_id}-{version}-{sink.id}"
        sink_template = EventStreamSinkTemplate(event_stream, dataset, version, sink)
        sink.cf_stack_template = sink_template.generate_stack_template()
        sink.cf_status = "CREATE_IN_PROGRESS"
        sink.cf_stack_name = sink_name
        event_stream.sinks.append(sink)
        self.cloudformation_client.create_stack(
            name=sink_name,
            template=sink.cf_stack_template.json(),
            tags=[{"Key": "created_by", "Value": updated_by}],
        )
        self.update_event_stream(event_stream, updated_by)
        return event_stream

    def delete_sink(self, dataset_id, version, sink_id, updated_by):
        event_stream = self.get_event_stream(dataset_id, version)
        if event_stream is None:
            raise ResourceNotFound
        if event_stream.deleted:
            raise ResourceNotFound

        sink = self.get_sink(event_stream, sink_id)
        sink.cf_status = "DELETE_IN_PROGRESS"
        sink.deleted = True
        self.cloudformation_client.delete_stack(sink.cf_stack_name)
        self.update_event_stream(event_stream, updated_by)

    def update_event_stream(self, event_stream: EventStream, updated_by: str):
        event_stream.config_version += 1
        event_stream.updated_by = updated_by
        event_stream.updated_at = datetime_utils.utc_now_with_timezone()
        self.event_streams_table.put_event_stream(event_stream)


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
        return {
            "SinkS3Resource": {
                "Type": "AWS::KinesisFirehose::DeliveryStream",
                "Properties": self.firehose_s3_delivery_stream(),
            },
            "SinkS3ResourceIAM": {
                "Type": "AWS::IAM::Role",
                "Properties": self.firehose_s3_iam(),
            },
        }

    def get_kinesis_source_stream(self):
        return f"dp.{self.dataset['confidentiality']}.{self.dataset['Id']}.{self.get_processing_stage()}.{self.version}.json"

    def get_kinesis_source_arn(self, kinesis_source_stream_name=None):
        if kinesis_source_stream_name is None:
            kinesis_source_stream_name = self.get_kinesis_source_stream()

        kinesis_source_base_arn = (
            "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream"
        )
        # Where are we reading data from:
        kinesis_source_arn = f"{kinesis_source_base_arn}/{kinesis_source_stream_name}"
        return kinesis_source_arn

    def get_processing_stage(self):
        # self.dataset does not contain processing_stage - for now default to "processed"
        return "processed"

    def get_output_prefix(self):
        return f"{self.get_processing_stage()}/{self.dataset['confidentiality']}/{self.dataset['Id']}/version={self.version}"

    def firehose_s3_delivery_stream(self):
        kinesis_source_stream_name = self.get_kinesis_source_stream()
        kinesis_source_arn = self.get_kinesis_source_arn(kinesis_source_stream_name)

        delivery_stream_name = (
            f"event-sink-{self.dataset['Id']}-{self.version}-{self.sink.id}"
        )

        output_bucket_arn = f"arn:aws:s3:::ok-origo-dataplatform-{ENV}"
        output_prefix = self.get_output_prefix()
        # https://docs.aws.amazon.com/firehose/latest/dev/s3-prefixes.html
        date_prefix = "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}"
        error_output = f"event-stream-sink/error/!{{firehose:error-output-type}}/{self.dataset['Id']}/{self.version}/{date_prefix}/"
        properties = {
            "DeliveryStreamName": delivery_stream_name,
            "DeliveryStreamType": "KinesisStreamAsSource",
            "KinesisStreamSourceConfiguration": {
                "KinesisStreamARN": {"Fn::Sub": kinesis_source_arn},
                "RoleARN": {"Fn::GetAtt": ["SinkS3ResourceIAM", "Arn"]},
            },
            "S3DestinationConfiguration": {
                "BucketARN": output_bucket_arn,
                "BufferingHints": {"IntervalInSeconds": 300, "SizeInMBs": 1},
                "ErrorOutputPrefix": error_output,
                "Prefix": f"{output_prefix}/{date_prefix}/",
                "RoleARN": {"Fn::GetAtt": ["SinkS3ResourceIAM", "Arn"]},
            },
        }
        """
        S3DestinationConfiguration also have the following (optional) options:
            "CloudWatchLoggingOptions" : CloudWatchLoggingOptions,
            "CompressionFormat" : String,
            "EncryptionConfiguration" : EncryptionConfiguration,
        """
        return properties

    def firehose_s3_iam(self):
        permission_boundary_arn = (
            "arn:aws:iam::${AWS::AccountId}:policy/oslokommune/oslokommune-boundary"
        )

        role_name = f"event-streams-{self.dataset['Id']}-{self.version}-sink-{self.sink.type}-role"[
            0:64
        ]
        policy_name = f"event-streams-{self.dataset['Id']}-{self.version}-sink-{self.sink.type}-policy"[
            0:128
        ]

        kinesis_source_arn = self.get_kinesis_source_arn()

        s3_bucket_arn = f"arn:aws:s3:::ok-origo-dataplatform-{ENV}"
        s3_bucket_prefix = self.get_output_prefix()
        s3_error_path = f"{s3_bucket_arn}/event-stream-sink/error/*"
        s3_output_path = f"{s3_bucket_arn}/{s3_bucket_prefix}/*"

        return {
            "PermissionsBoundary": {"Fn::Sub": permission_boundary_arn},
            "RoleName": role_name,
            "Tags": [
                {"Key": "datasetId", "Value": self.dataset["Id"]},
                {"Key": "version", "Value": self.version},
            ],
            "AssumeRolePolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "firehose.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            },
            "Policies": [
                {
                    "PolicyName": policy_name,
                    "PolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "kinesis:GetRecords",
                                    "kinesis:GetShardIterator",
                                    "kinesis:DescribeStream",
                                    "kinesis:ListStreams",
                                ],
                                "Resource": {"Fn::Sub": kinesis_source_arn},
                            },
                            {
                                "Effect": "Allow",
                                "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                                "Resource": s3_bucket_arn,
                            },
                            {
                                "Effect": "Allow",
                                "Action": ["s3:PutObject"],
                                "Resource": [s3_error_path, s3_output_path],
                            },
                        ]
                    },
                }
            ],
        }

    def firehose_elasticsearch(self):
        properties = {}
        return {
            "SinkS3Resource": {
                "Type": "AWS::KinesisFirehose::DeliveryStream",
                "Properties": properties,
            }
        }
