from database import EventStream

dataset_id = "my-dataset-id"
version = "1"
confidentiality = "green"
updated_by = "pompelogpilt"
elasticsearch_sink_id = "abcde"
s3_sink_id = "edcba"
created_at = "2020-01-21T09:28:57.831435+00:00"

sink_elasticsearch_cf_template = {
    "Description": f"Firehose for {dataset_id}/{version}: elasticsearch",
    "Resources": {
        "SinkElasticsearchResource": {
            "Type": "AWS::KinesisFirehose::DeliveryStream",
            "Properties": {
                "DeliveryStreamName": f"event-sink-{dataset_id}-{version}-{elasticsearch_sink_id}",
                "DeliveryStreamType": "KinesisStreamAsSource",
                "KinesisStreamSourceConfiguration": {
                    "KinesisStreamARN": {
                        "Fn::Sub": f"arn:aws:kinesis:${{AWS::Region}}:${{AWS::AccountId}}:stream/dp.green.{dataset_id}.processed.{version}.json"
                    },
                    "RoleARN": {"Fn::GetAtt": ["SinkElasticsearchResourceIAM", "Arn"]},
                },
                "ElasticsearchDestinationConfiguration": {
                    "BufferingHints": {"IntervalInSeconds": 300, "SizeInMBs": 1},
                    "DomainARN": {
                        "Fn::Sub": "arn:aws:es:${AWS::Region}:${AWS::AccountId}:domain/dataplatform-eventdata"
                    },
                    "IndexName": f"processed-green-{dataset_id}-{version}",
                    "IndexRotationPeriod": "OneMonth",
                    "RetryOptions": {"DurationInSeconds": 5},
                    "RoleARN": {"Fn::GetAtt": ["SinkElasticsearchResourceIAM", "Arn"]},
                    "TypeName": "",
                    "S3BackupMode": "FailedDocumentsOnly",
                    "S3Configuration": {
                        "BucketARN": "arn:aws:s3:::ok-origo-dataplatform-localdev",
                        "BufferingHints": {"IntervalInSeconds": 300, "SizeInMBs": 1},
                        "ErrorOutputPrefix": f"event-stream-sink/error/!{{firehose:error-output-type}}/{dataset_id}/{version}/year=!{{timestamp:yyyy}}/month=!{{timestamp:MM}}/day=!{{timestamp:dd}}/hour=!{{timestamp:HH}}/",
                        "Prefix": f"processed/green/{dataset_id}/version={version}/year=!{{timestamp:yyyy}}/month=!{{timestamp:MM}}/day=!{{timestamp:dd}}/hour=!{{timestamp:HH}}/",
                        "RoleARN": {
                            "Fn::GetAtt": [
                                "SinkElasticsearchS3BackupResourceIAM",
                                "Arn",
                            ]
                        },
                    },
                },
            },
        },
        "SinkElasticsearchResourceIAM": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "PermissionsBoundary": {
                    "Fn::Sub": "arn:aws:iam::${AWS::AccountId}:policy/oslokommune/oslokommune-boundary"
                },
                "RoleName": f"stream-{dataset_id}-{version}-{elasticsearch_sink_id}-es",
                "Tags": [
                    {"Key": "datasetId", "Value": dataset_id},
                    {"Key": "version", "Value": version},
                ],
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "firehose.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ]
                },
                "Policies": [
                    {
                        "PolicyName": f"stream-{dataset_id}-{version}-{elasticsearch_sink_id}-es",
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
                                    "Resource": {
                                        "Fn::Sub": f"arn:aws:kinesis:${{AWS::Region}}:${{AWS::AccountId}}:stream/dp.green.{dataset_id}.processed.{version}.json"
                                    },
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": ["es:DescribeElasticsearchDomain"],
                                    "Resource": {
                                        "Fn::Sub": "arn:aws:es:${AWS::Region}:${AWS::AccountId}:domain/dataplatform-eventdata"
                                    },
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": ["es:ESHttpPost"],
                                    "Resource": {
                                        "Fn::Sub": f"arn:aws:es:${{AWS::Region}}:${{AWS::AccountId}}:domain/dataplatform-eventdata/processed-green-{dataset_id}-{version}*"
                                    },
                                },
                            ]
                        },
                    }
                ],
            },
        },
        "SinkElasticsearchS3BackupResourceIAM": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "PermissionsBoundary": {
                    "Fn::Sub": "arn:aws:iam::${AWS::AccountId}:policy/oslokommune/oslokommune-boundary"
                },
                "RoleName": f"stream-{dataset_id}-{version}-{elasticsearch_sink_id}-backup",
                "Tags": [
                    {"Key": "datasetId", "Value": dataset_id},
                    {"Key": "version", "Value": version},
                ],
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "firehose.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ]
                },
                "Policies": [
                    {
                        "PolicyName": f"stream-{dataset_id}-{version}-{elasticsearch_sink_id}-backup",
                        "PolicyDocument": {
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                                    "Resource": "arn:aws:s3:::ok-origo-dataplatform-localdev",
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": ["s3:PutObject"],
                                    "Resource": [
                                        "arn:aws:s3:::ok-origo-dataplatform-localdev/event-stream-sink/error/*",
                                        f"arn:aws:s3:::ok-origo-dataplatform-localdev/processed/green/{dataset_id}/version={version}/*",
                                    ],
                                },
                            ]
                        },
                    }
                ],
            },
        },
    },
}

sink_s3_cf_template = {
    "Description": f"Firehose for {dataset_id}/{version}: s3",
    "Resources": {
        "SinkS3Resource": {
            "Type": "AWS::KinesisFirehose::DeliveryStream",
            "Properties": {
                "DeliveryStreamName": f"event-sink-{dataset_id}-{version}-{s3_sink_id}",
                "DeliveryStreamType": "KinesisStreamAsSource",
                "KinesisStreamSourceConfiguration": {
                    "KinesisStreamARN": {
                        "Fn::Sub": f"arn:aws:kinesis:${{AWS::Region}}:${{AWS::AccountId}}:stream/dp.green.{dataset_id}.processed.{version}.json"
                    },
                    "RoleARN": {"Fn::GetAtt": ["SinkS3ResourceIAM", "Arn"]},
                },
                "S3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::ok-origo-dataplatform-localdev",
                    "BufferingHints": {"IntervalInSeconds": 300, "SizeInMBs": 1},
                    "ErrorOutputPrefix": f"event-stream-sink/error/!{{firehose:error-output-type}}/{dataset_id}/{version}/year=!{{timestamp:yyyy}}/month=!{{timestamp:MM}}/day=!{{timestamp:dd}}/hour=!{{timestamp:HH}}/",
                    "Prefix": f"processed/green/{dataset_id}/version={version}/year=!{{timestamp:yyyy}}/month=!{{timestamp:MM}}/day=!{{timestamp:dd}}/hour=!{{timestamp:HH}}/",
                    "RoleARN": {"Fn::GetAtt": ["SinkS3ResourceIAM", "Arn"]},
                },
            },
        },
        "SinkS3ResourceIAM": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "PermissionsBoundary": {
                    "Fn::Sub": "arn:aws:iam::${AWS::AccountId}:policy/oslokommune/oslokommune-boundary"
                },
                "RoleName": f"stream-{dataset_id}-{version}-{s3_sink_id}-s3",
                "Tags": [
                    {"Key": "datasetId", "Value": dataset_id},
                    {"Key": "version", "Value": version},
                ],
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "firehose.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ]
                },
                "Policies": [
                    {
                        "PolicyName": f"stream-{dataset_id}-{version}-{s3_sink_id}-s3",
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
                                    "Resource": {
                                        "Fn::Sub": f"arn:aws:kinesis:${{AWS::Region}}:${{AWS::AccountId}}:stream/dp.green.{dataset_id}.processed.{version}.json"
                                    },
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                                    "Resource": "arn:aws:s3:::ok-origo-dataplatform-localdev",
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": ["s3:PutObject"],
                                    "Resource": [
                                        "arn:aws:s3:::ok-origo-dataplatform-localdev/event-stream-sink/error/*",
                                        f"arn:aws:s3:::ok-origo-dataplatform-localdev/processed/green/{dataset_id}/version={version}/*",
                                    ],
                                },
                            ]
                        },
                    }
                ],
            },
        },
    },
}

cf_dummy_template = {
    "Description": "foo",
    "Resources": {"foo": {"type": "bar", "properties": {"foo": "bar"}}},
}

event_stream = EventStream(
    **{
        "cf_stack_template": cf_dummy_template,
        "cf_status": "CREATE_IN_PROGRESS",
        "cf_stack_name": f"event-stream-{dataset_id}-{version}",
        "id": f"{dataset_id}/{version}",
        "create_raw": True,
        "updated_by": updated_by,
        "updated_at": created_at,
        "deleted": False,
        "sinks": [],
        "subscribable": {
            "enabled": True,
            "cf_stack_template": cf_dummy_template,
            "cf_stack_name": f"event-subscribable-{dataset_id}-{version}",
            "cf_status": "CREATE_IN_PROGRESS",
        },
    }
)
