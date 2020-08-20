import os

from services import datetime_utils, EventService
from services.exceptions import (
    ResourceNotFound,
    ParentResourceNotReady,
    ResourceConflict,
)
from database import Subscribable, StackTemplate


event_publisher_lambda_name = (
    f"event-data-subscription-{os.environ['ORIGO_ENVIRONMENT']}-publish_event"
)


class SubscribableService(EventService):
    def get_subscribable(self, dataset_id, version):
        event_stream = self.get_event_stream(dataset_id, version)

        if not event_stream or event_stream.deleted:
            raise ResourceNotFound

        return event_stream.subscribable

    def enable_subscribable(self, dataset_id, version, updated_by):
        event_stream = self.get_event_stream(dataset_id, version)

        if not event_stream or event_stream.deleted:
            raise ResourceNotFound
        if not event_stream.is_active:
            raise ParentResourceNotReady
        if event_stream.subscribable.enabled:
            raise ResourceConflict

        subscribable = Subscribable(
            cf_stack_template=generate_subscribable_cf_template(
                dataset_id=dataset_id,
                version=version,
                dataset_confidentiality=self.dataset_client.get_dataset(dataset_id)[
                    "confidentiality"
                ],
            ),
            cf_status="CREATE_IN_PROGRESS",
            enabled=True,
            updated_by=updated_by,
            updated_at=datetime_utils.utc_now_with_timezone(),
        )
        subscribable.cf_stack_name = subscribable.get_stack_name(dataset_id, version)
        event_stream.subscribable = subscribable

        self.update_event_stream(event_stream, updated_by)

        self.cloudformation_client.create_stack(
            name=subscribable.cf_stack_name,
            template=event_stream.subscribable.cf_stack_template.json(),
            tags=[{"Key": "created_by", "Value": updated_by}],
        )

        return event_stream.subscribable

    def disable_subscribable(self, dataset_id, version, updated_by):
        event_stream = self.get_event_stream(dataset_id, version)

        if not event_stream or event_stream.deleted:
            raise ResourceNotFound
        if not event_stream.subscribable.enabled:
            raise ResourceConflict

        event_stream.subscribable.cf_status = "DELETE_IN_PROGRESS"
        event_stream.subscribable.enabled = False

        self.update_event_stream(event_stream, updated_by)

        self.cloudformation_client.delete_stack(
            name=event_stream.subscribable.cf_stack_name
        )

        return event_stream.subscribable


def generate_subscribable_cf_template(dataset_id, version, dataset_confidentiality):
    stream_name = f"dp.{dataset_confidentiality}.{dataset_id}.processed.{version}.json"

    return StackTemplate(
        **{
            "Description": f"Subscription event source mapping for {dataset_id}/{version}",
            "Resources": {
                "SubscriptionSource": {
                    "Type": "AWS::Lambda::EventSourceMapping",
                    "Properties": {
                        "BatchSize": 10,
                        "Enabled": True,
                        "EventSourceArn": {
                            "Fn::Sub": "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream/"
                            + stream_name
                        },
                        "FunctionName": {
                            "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:"
                            + event_publisher_lambda_name
                        },
                        "StartingPosition": "LATEST",
                    },
                }
            },
        }
    )
