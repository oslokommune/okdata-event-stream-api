from database import EventStream
from clients.keycloak_config import KeycloakConfig

dataset_id = "some-dataset-id"
version = "1"
confidentiality = "green"
updated_by = "larsmonsen"
utc_now = "2020-01-21T09:28:57.831435+00:00"

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
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:pipeline-router-localdev-route"
                },
                "StartingPosition": "LATEST",
            },
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
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:pipeline-router-localdev-route"
                },
                "StartingPosition": "LATEST",
            },
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
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:pipeline-router-localdev-route"
                },
                "StartingPosition": "LATEST",
            },
        },
    },
}

event_stream = EventStream(
    **{
        "cf_stack_template": processed_and_raw_cf_template,
        "cf_status": "CREATE_IN_PROGRESS",
        "id": "some-dataset-id/1",
        "create_raw": True,
        "updated_by": "larsmonsen",
        "updated_at": "2020-01-21T09:28:57.831435",
        "deleted": False,
        "sinks": [],
        "subscribable": {
            "cf_stack_template": None,
            "cf_status": "INACTIVE",
            "enabled": False,
        },
    }
)


ssm_parameters = KeycloakConfig("mock", "mock", "mock", "mock")
