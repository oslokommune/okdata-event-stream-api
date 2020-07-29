from database import EventStream

from .stream import processed_and_raw_cf_template


dataset_id = "some-dataset-id"
version = "1"
confidentiality = "green"

subscribable_cf_template = {
    "Description": "User generated resources for subscribable event stream",
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
                "FunctionName": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:event-data-subscription-dev-publish_event",
                "StartingPosition": "LATEST",
            },
        }
    },
}

subscribable_event_stream = EventStream(
    **{
        "cf_stack_template": processed_and_raw_cf_template,
        "cf_status": "ACTIVE",
        "id": "some-dataset-id/1",
        "create_raw": True,
        "updated_by": "larsmonsen",
        "updated_at": "2020-01-21T09:28:57.831435",
        "deleted": False,
        "subscribable": {
            "cf_stack_template": subscribable_cf_template,
            "cf_status": "CREATE_IN_PROGRESS",
            "enabled": False,
        },
        "sinks": [],
    }
)
