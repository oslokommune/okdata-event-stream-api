from database import EventStream
from clients.keycloak_config import KeycloakConfig

from .stream import processed_and_raw_cf_template


dataset_id = "some-dataset-id"
version = "1"
access_rights = "public"
confidentiality = "green"
updated_by = "tyholtapenes"
utc_now = "2020-01-30T09:28:57.831435+00:00"

subscribable_cf_template = {
    "Description": f"Subscription event source mapping for {dataset_id}/{version}",
    "Resources": {
        "SubscriptionSource": {
            "Type": "AWS::Lambda::EventSourceMapping",
            "Properties": {
                "BatchSize": 10,
                "Enabled": True,
                "EventSourceArn": {
                    "Fn::Sub": "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream/"
                    + f"dp.{confidentiality}.{dataset_id}.processed.{version}.json"
                },
                "FunctionName": {
                    "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:event-data-subscription-localdev-publish_event"
                },
                "StartingPosition": "LATEST",
            },
        }
    },
}

event_stream = EventStream(
    **{
        "cf_stack_template": processed_and_raw_cf_template,
        "cf_stack_name": f"event-stream-{dataset_id}-{version}",
        "cf_status": "ACTIVE",
        "id": f"{dataset_id}/{version}",
        "create_raw": True,
        "updated_by": "larsmonsen",
        "updated_at": "2020-01-21T09:28:57.831435",
        "deleted": False,
        "sinks": [],
    }
)

subscribable_event_stream = EventStream(
    **{
        "cf_stack_template": processed_and_raw_cf_template,
        "cf_stack_name": f"event-stream-{dataset_id}-{version}",
        "cf_status": "ACTIVE",
        "id": f"{dataset_id}/{version}",
        "create_raw": True,
        "updated_by": "larsmonsen",
        "updated_at": "2020-01-21T09:28:57.831435",
        "deleted": False,
        "subscribable": {
            "cf_stack_template": subscribable_cf_template,
            "cf_stack_name": f"event-subscribable-{dataset_id}-{version}",
            "cf_status": "ACTIVE",
            "enabled": True,
        },
        "sinks": [],
    }
)

event_stream_not_ready = EventStream(
    **{
        "cf_stack_template": processed_and_raw_cf_template,
        "cf_stack_name": f"event-stream-{dataset_id}-{version}",
        "cf_status": "CREATE_IN_PROGRESS",
        "id": f"{dataset_id}/{version}",
        "create_raw": True,
        "updated_by": "larsmonsen",
        "updated_at": "2020-01-21T09:28:57.831435",
        "deleted": False,
        "sinks": [],
        "subscribable": {
            "cf_stack_template": None,
            "cf_stack_name": f"event-subscribable-{dataset_id}-{version}",
            "cf_status": "INACTIVE",
            "enabled": False,
        },
    }
)

ssm_parameters = KeycloakConfig("mock", "mock", "mock", "mock")
