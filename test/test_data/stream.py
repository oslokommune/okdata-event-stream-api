from database import EventStream
from clients.keycloak_config import KeycloakConfig

dataset_id = "some-dataset-id"
version = "1"
sink_id = "727qH"
accessRights = "public"
confidentiality = "green"
updated_by = "larsmonsen"
created_at = "2020-01-21T09:28:57.831435+00:00"
deleted_at = "2020-02-21T09:28:57.831435+00:00"

processed_and_raw_cf_template = {
    "Description": f"Kinesis streams and pipeline triggers for {dataset_id}/{version}",
    "Resources": {
        "RawDataStream": {
            "Type": "AWS::Kinesis::Stream",
            "Properties": {
                "Name": f"dp.{confidentiality}.{dataset_id}.raw.{version}.json",
                "ShardCount": 1,
                "Tags": [{"Key": "created_by", "Value": updated_by}],
            },
        },
        "RawPipelineTrigger": {
            "Type": "AWS::Lambda::EventSourceMapping",
            "Properties": {
                "BatchSize": 10,
                "Enabled": True,
                "EventSourceArn": {
                    "Fn::Sub": "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream/"
                    + f"dp.{confidentiality}.{dataset_id}.raw.{version}.json"
                },
                "FunctionName": {
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:pipeline-router-localdev-route-kinesis"
                },
                "MaximumBatchingWindowInSeconds": 10,
                "StartingPosition": "LATEST",
            },
            "DependsOn": "RawDataStream",
        },
        "ProcessedDataStream": {
            "Type": "AWS::Kinesis::Stream",
            "Properties": {
                "Name": f"dp.{confidentiality}.{dataset_id}.processed.{version}.json",
                "ShardCount": 1,
                "Tags": [{"Key": "created_by", "Value": updated_by}],
            },
        },
        "ProcessedPipelineTrigger": {
            "Type": "AWS::Lambda::EventSourceMapping",
            "Properties": {
                "BatchSize": 10,
                "Enabled": True,
                "EventSourceArn": {
                    "Fn::Sub": "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream/"
                    + f"dp.{confidentiality}.{dataset_id}.processed.{version}.json"
                },
                "FunctionName": {
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:pipeline-router-localdev-route-kinesis"
                },
                "MaximumBatchingWindowInSeconds": 10,
                "StartingPosition": "LATEST",
            },
            "DependsOn": "ProcessedDataStream",
        },
    },
}

processed_only_template = {
    "Description": f"Kinesis streams and pipeline triggers for {dataset_id}/{version}",
    "Resources": {
        "ProcessedDataStream": {
            "Type": "AWS::Kinesis::Stream",
            "Properties": {
                "Name": f"dp.{confidentiality}.{dataset_id}.processed.{version}.json",
                "ShardCount": 1,
                "Tags": [{"Key": "created_by", "Value": updated_by}],
            },
        },
        "ProcessedPipelineTrigger": {
            "Type": "AWS::Lambda::EventSourceMapping",
            "Properties": {
                "BatchSize": 10,
                "Enabled": True,
                "EventSourceArn": {
                    "Fn::Sub": "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream/"
                    + f"dp.{confidentiality}.{dataset_id}.processed.{version}.json"
                },
                "FunctionName": {
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:pipeline-router-localdev-route-kinesis"
                },
                "MaximumBatchingWindowInSeconds": 10,
                "StartingPosition": "LATEST",
            },
            "DependsOn": "ProcessedDataStream",
        },
    },
}

event_stream = EventStream(
    **{
        "cf_stack_template": processed_and_raw_cf_template,
        "cf_status": "CREATE_IN_PROGRESS",
        "cf_stack_name": f"event-stream-{dataset_id}-{version}",
        "id": f"{dataset_id}/{version}",
        "create_raw": True,
        "updated_by": "larsmonsen",
        "updated_at": created_at,
        "deleted": False,
        "sinks": [],
        "subscribable": {
            "cf_stack_template": None,
            "cf_status": "INACTIVE",
            "cf_stack_name": f"event-subscribable-{dataset_id}-{version}",
            "enabled": False,
        },
    }
)

deleted_event_stream = EventStream(
    **{
        "cf_stack_template": processed_and_raw_cf_template,
        "cf_status": "INACTIVE",
        "cf_stack_name": f"event-stream-{dataset_id}-{version}",
        "config_version": 2,
        "id": f"{dataset_id}/{version}",
        "create_raw": True,
        "updated_by": "larsmonsen",
        "updated_at": deleted_at,
        "deleted": True,
        "sinks": [],
        "subscribable": {
            "cf_stack_template": None,
            "cf_stack_name": f"event-subscribable-{dataset_id}-{version}",
            "cf_status": "INACTIVE",
            "enabled": False,
        },
    }
)

event_stream_with_subresources = EventStream(
    **{
        "cf_stack_template": processed_and_raw_cf_template,
        "cf_status": "CREATE_IN_PROGRESS",
        "cf_stack_name": f"event-stream-{dataset_id}-{version}",
        "id": f"{dataset_id}/{version}",
        "create_raw": True,
        "updated_by": "larsmonsen",
        "updated_at": created_at,
        "deleted": False,
        "sinks": [],
        "subscribable": {
            "cf_stack_template": None,
            "cf_stack_name": f"event-subscribable-{dataset_id}-{version}",
            "cf_status": "ACTIVE",
            "enabled": True,
        },
    }
)


ssm_parameters = KeycloakConfig("mock", "mock", "mock", "mock")
