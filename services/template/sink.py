import os

from database import EventStream, Sink, SinkType, StackTemplate
from util import CONFIDENTIALITY_MAP

ENV = os.environ["ORIGO_ENVIRONMENT"]


class SinkTemplate:
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
        return "year=!{timestamp:yyyy}/month=!{timestamp:M}/day=!{timestamp:d}/hour=!{timestamp:H}"

    def get_error_output(self) -> str:
        return f"event-stream-sink/error/!{{firehose:error-output-type}}/{self.dataset['Id']}/{self.version}"

    ##### Kinesis source related functions #####
    def get_kinesis_source_stream(self) -> str:
        """Return the Kinesis source name. This is where we read data from."""
        confidentiality = CONFIDENTIALITY_MAP[self.dataset["accessRights"]]
        return f"dp.{confidentiality}.{self.dataset['Id']}.{self.get_processing_stage()}.{self.version}.json"

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
        """Return the S3 output prefix.

        The prefix follows standard dataset output format up until the version
        (no edition).
        """
        confidentiality = CONFIDENTIALITY_MAP[self.dataset["accessRights"]]
        return f"{self.get_processing_stage()}/{confidentiality}/{self.dataset['Id']}/version={self.version}"

    ##### Elasticsearch related functions
    def get_elasticsearch_index_name(self) -> str:
        """Return the base name for the Elasticsearch index where we will push data.

        Since we are rotating index the actual Elasticsearch index will have a
        {year}-{month} postfix.
        """
        confidentiality = CONFIDENTIALITY_MAP[self.dataset["accessRights"]]
        index = f"{self.get_processing_stage()}-{confidentiality}-{self.dataset['Id']}-{self.version}"
        return index.lower()  # ES indexes MUST be lower

    def get_elasticsearch_destination_domain(self) -> str:
        return "dataplatform-eventdata"

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

    def get_iam_role_name(self, key):
        return self.get_iam_name(key, 64)

    def get_iam_policy_name(self, key):
        return self.get_iam_name(key, 128)

    def get_iam_name(self, key, max_length) -> str:
        prefix = f"stream-{self.dataset['Id']}"
        suffix = f"-{self.version}-{self.sink.id}-{key}"
        prefix_length = max_length - len(suffix)
        return prefix[0:prefix_length] + suffix

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
