import os
from origo.data.dataset import Dataset
from database import EventStreamsTable, EventStream, Sink, SinkType, StackTemplate
from clients import CloudformationClient
from services import (
    ResourceNotFound,
    ResourceConflict,
    SubResourceNotFound,
    ResourceUnderConstruction,
    ResourceUnderDeletion,
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
        self.dataset_client = dataset_client
        self.event_streams_table = EventStreamsTable()
        self.cloudformation_client = CloudformationClient()

    def get_event_stream(self, dataset_id: str, version: str):
        event_stream_id = f"{dataset_id}/{version}"
        return self.event_streams_table.get_event_stream(event_stream_id)

    def get_sinks(self, dataset_id: str, version: str) -> list:
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

    def check_for_existing_sink_type(
        self, event_stream: EventStream, sink_type: SinkType
    ) -> bool:
        for sink in event_stream.sinks:
            if sink.type == sink_type and sink.cf_status == "DELETE_IN_PROGRESS":
                raise ResourceUnderDeletion
            elif sink.type == sink_type.value and sink.deleted is False:
                raise ResourceConflict(
                    f"Sink: {sink_type.value} already exists on {event_stream.id}"
                )

    def add_sink(
        self, dataset_id: str, version: str, sink_data: dict, updated_by: str
    ) -> Sink:
        event_stream = self.get_event_stream(dataset_id, version)
        if event_stream is None:
            raise ResourceNotFound
        if event_stream.deleted:
            raise ResourceNotFound

        sink_type = SinkType[sink_data["type"].upper()]
        self.check_for_existing_sink_type(event_stream, sink_type)

        sink = Sink(type=sink_type.value)
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
        return sink

    def delete_sink(self, dataset_id: str, version: str, sink_id: str, updated_by: str):
        event_stream = self.get_event_stream(dataset_id, version)
        if event_stream is None:
            raise ResourceNotFound
        if event_stream.deleted:
            raise ResourceNotFound

        sink = self.get_sink(dataset_id, version, sink_id)
        if sink.cf_status == "CREATE_IN_PROGRESS":
            raise ResourceUnderConstruction
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

    ##### Generic/Shared functions #####
    def get_processing_stage(self) -> str:
        # self.dataset does not contain processing_stage - for now default to "processed"
        return "processed"

    def get_delivery_stream_name(self) -> str:
        return f"event-sink-{self.dataset['Id']}-{self.version}-{self.sink.id}"

    def get_date_prefix(self) -> str:
        # https://docs.aws.amazon.com/firehose/latest/dev/s3-prefixes.html
        return "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}"

    def get_error_output(self) -> str:
        return f"event-stream-sink/error/!{{firehose:error-output-type}}/{self.dataset['Id']}/{self.version}"

    ##### Kinesis source related functions #####
    def get_kinesis_source_stream(self) -> str:
        # Kinesis source name, this is where we read data from
        return f"dp.{self.dataset['confidentiality']}.{self.dataset['Id']}.{self.get_processing_stage()}.{self.version}.json"

    def get_kinesis_source_arn(self) -> str:
        # Full ARN for the source stream
        kinesis_source_stream_name = self.get_kinesis_source_stream()

        kinesis_source_base_arn = (
            "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream"
        )
        kinesis_source_arn = f"{kinesis_source_base_arn}/{kinesis_source_stream_name}"
        return kinesis_source_arn

    ##### S3 related functions #####
    def get_s3_output_bucket_arn(self) -> str:
        return f"arn:aws:s3:::ok-origo-dataplatform-{ENV}"

    def get_s3_output_prefix(self) -> str:
        # Output path follows standard dataset output up until the version (no edition)
        return f"{self.get_processing_stage()}/{self.dataset['confidentiality']}/{self.dataset['Id']}/version={self.version}"

    ##### Elasticsearch related functions
    def get_elasticsearch_index_name(self) -> str:
        # Base name for Elasticsearch index where we will push data
        # Since we are rotating index the actual Elasticsearch index will have a {year}-{month} postfix
        index = f"{self.get_processing_stage()}-{self.dataset['confidentiality']}-{self.dataset['Id']}-{self.version}"
        return index.lower()  # ES indexes MUST be lower

    def get_elasticsearch_destination_domain(self) -> str:
        return "dataplatform-event-sink"

    def get_elasticsearch_destination_arn(self) -> str:
        domain = self.get_elasticsearch_destination_domain()
        base_arn = "arn:aws:es:${AWS::Region}:${AWS::AccountId}:domain"
        return f"{base_arn}/{domain}"

    def get_elasticsearch_destination_index_arn(self) -> str:
        domain = self.get_elasticsearch_destination_domain()
        es_index = self.get_elasticsearch_index_name()
        base_arn = "arn:aws:es:${AWS::Region}:${AWS::AccountId}:domain"
        return f"{base_arn}/{domain}/{es_index}*"

    ##### IAM related functions #####
    def get_iam_buffering_hints(self) -> dict:
        interval = 300
        size = 1
        if ENV == "dev":
            interval = 60

        return {"IntervalInSeconds": interval, "SizeInMBs": size}

    def get_iam_permission_boundary(self) -> str:
        permission_boundary_arn = (
            "arn:aws:iam::${AWS::AccountId}:policy/oslokommune/oslokommune-boundary"
        )
        return permission_boundary_arn

    def get_iam_role_name(self, key) -> str:
        role_name = f"stream-{self.dataset['Id']}-{self.version}-{self.sink.id}-{key}"
        return role_name[0:64]

    def get_iam_policy_name(self, key) -> str:
        policy_name = (
            f"streams-{self.dataset['Id']}-{self.version}-{self.sink.id}-{key}"
        )
        return policy_name[0:128]

    ##### Main template #####
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

    ##### S3 Firehose #####
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

    def firehose_s3_delivery_stream(self) -> dict:
        """
        Create a S3 delivery stream where we:
        * Read from a named kinesis event-stream
        * Write to a named output path in S3
        """
        date_prefix = self.get_date_prefix()
        error_output_base = self.get_error_output()
        error_output = f"{error_output_base}/{date_prefix}/"

        properties = {
            "DeliveryStreamName": self.get_delivery_stream_name(),
            "DeliveryStreamType": "KinesisStreamAsSource",
            "KinesisStreamSourceConfiguration": {
                "KinesisStreamARN": {"Fn::Sub": self.get_kinesis_source_arn()},
                "RoleARN": {"Fn::GetAtt": ["SinkS3ResourceIAM", "Arn"]},
            },
            "S3DestinationConfiguration": {
                "BucketARN": self.get_s3_output_bucket_arn(),
                "BufferingHints": self.get_iam_buffering_hints(),
                "ErrorOutputPrefix": error_output,
                "Prefix": f"{self.get_s3_output_prefix()}/{date_prefix}/",
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

    def firehose_s3_iam(self) -> dict:
        """
        The IAM policy for a S3 firehose to be able to:
            * Read from a named Kinesis event-stream
            * Locate the destination bucket
            * Write to a named output path in S3
                The destination resource have a wildcard to be able to write to year=XXXX/month=XX/day=XX
        """
        s3_bucket_arn = self.get_s3_output_bucket_arn()
        s3_error_path = f"{s3_bucket_arn}/event-stream-sink/error/*"
        s3_output_path = f"{s3_bucket_arn}/{self.get_s3_output_prefix()}/*"

        return {
            "PermissionsBoundary": {"Fn::Sub": self.get_iam_permission_boundary()},
            "RoleName": self.get_iam_role_name(key="s3"),
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
                    "PolicyName": self.get_iam_policy_name(key="s3"),
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
                                "Resource": {"Fn::Sub": self.get_kinesis_source_arn()},
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

    ##### Elasticsearch Firehose #####
    def firehose_elasticsearch(self) -> dict:
        return {
            "SinkElasticsearchResource": {
                "Type": "AWS::KinesisFirehose::DeliveryStream",
                "Properties": self.firehose_elasticsearch_delivery_stream(),
            },
            "SinkElasticsearchResourceIAM": {
                "Type": "AWS::IAM::Role",
                "Properties": self.firehose_elasticsearch_iam(),
            },
            "SinkElasticsearchS3BackupResourceIAM": {
                "Type": "AWS::IAM::Role",
                "Properties": self.firehose_elasticsearch_backup_iam(),
            },
        }

    def firehose_elasticsearch_delivery_stream(self) -> dict:
        """
        Create a Elasticsearch delivery stream where we:
        * Read from a named kinesis event-stream
        * Write to a named Elasticsearch domain on a named Elasticsearch index
        """
        date_prefix = self.get_date_prefix()
        error_output_base = self.get_error_output()
        error_output = f"{error_output_base}/{date_prefix}/"

        properties = {
            "DeliveryStreamName": self.get_delivery_stream_name(),
            "DeliveryStreamType": "KinesisStreamAsSource",
            "KinesisStreamSourceConfiguration": {
                "KinesisStreamARN": {"Fn::Sub": self.get_kinesis_source_arn()},
                "RoleARN": {"Fn::GetAtt": ["SinkElasticsearchResourceIAM", "Arn"]},
            },
            "ElasticsearchDestinationConfiguration": {
                "BufferingHints": self.get_iam_buffering_hints(),
                "DomainARN": {"Fn::Sub": self.get_elasticsearch_destination_arn()},
                "IndexName": self.get_elasticsearch_index_name(),
                "IndexRotationPeriod": "OneMonth",  # NoRotation | OneDay | OneHour | OneMonth | OneWeek
                "RetryOptions": {"DurationInSeconds": 5},
                "RoleARN": {"Fn::GetAtt": ["SinkElasticsearchResourceIAM", "Arn"]},
                "TypeName": "",  # Must be empty for ES 7.7
                "S3BackupMode": "FailedDocumentsOnly",
                "S3Configuration": {
                    "BucketARN": self.get_s3_output_bucket_arn(),
                    "BufferingHints": self.get_iam_buffering_hints(),
                    "ErrorOutputPrefix": error_output,
                    "Prefix": f"{self.get_s3_output_prefix()}/{date_prefix}/",
                    "RoleARN": {
                        "Fn::GetAtt": ["SinkElasticsearchS3BackupResourceIAM", "Arn"]
                    },
                },
            },
        }
        """
        ElasticsearchDestinationConfiguration also have the following (optional) options:
            "ProcessingConfiguration" : ProcessingConfiguration
            "VpcConfiguration" : VpcConfiguration
        """
        return properties

    def firehose_elasticsearch_iam(self) -> dict:
        """
        The IAM policy for a Elasticsearch firehose to be able to:
            * Read from a named Kinesis event-stream
            * Describe a Elasticsearch domain
            * Do a POST to that domain on a given index (index name is based on event-stream name)
                The destination resource have a wildcard to be able to rotate index on year+month
        """
        return {
            "PermissionsBoundary": {"Fn::Sub": self.get_iam_permission_boundary()},
            "RoleName": self.get_iam_role_name(key="es"),
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
                    "PolicyName": self.get_iam_policy_name(key="es"),
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
                                "Resource": {"Fn::Sub": self.get_kinesis_source_arn()},
                            },
                            {
                                "Effect": "Allow",
                                "Action": ["es:DescribeElasticsearchDomain"],
                                "Resource": {
                                    "Fn::Sub": self.get_elasticsearch_destination_arn()
                                },
                            },
                            {
                                "Effect": "Allow",
                                "Action": ["es:ESHttpPost"],
                                "Resource": {
                                    "Fn::Sub": self.get_elasticsearch_destination_index_arn()
                                },
                            },
                        ]
                    },
                },
            ],
        }

    def firehose_elasticsearch_backup_iam(self) -> dict:
        """
        The Firehose Elasticsearch delivery stream will write a error message to S3 if the
        delivery to Elasticsearch fails. It can also deliver a backup to S3, this
        IAM role will also give the delivery stream the ability to write to that destination
        (see ElasticsearchDestinationConfiguration --> S3BackupMode)
        """
        s3_bucket_arn = self.get_s3_output_bucket_arn()
        s3_error_path = f"{s3_bucket_arn}/event-stream-sink/error/*"
        s3_output_path = f"{s3_bucket_arn}/{self.get_s3_output_prefix()}/*"

        return {
            "PermissionsBoundary": {"Fn::Sub": self.get_iam_permission_boundary()},
            "RoleName": self.get_iam_role_name(key="backup"),
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
                    "PolicyName": self.get_iam_policy_name(key="backup"),
                    "PolicyDocument": {
                        "Statement": [
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
