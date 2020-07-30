import os

from origo.data.dataset import Dataset

from clients import CloudformationClient
from services import ResourceNotFound, ResourceConflict, datetime_utils
from database import EventStreamsTable, Subscribable, StackTemplate, CfStackType


event_publisher_lambda_name = (
    f"event-data-subscription-{os.environ['ORIGO_ENVIRONMENT']}-publish_event"
)


class SubscribableService:
    def __init__(self, dataset_client: Dataset):
        self.dataset_client = dataset_client
        self.cloudformation_client = CloudformationClient()
        self.event_streams_table = EventStreamsTable()

    def get_subscribable(self, dataset_id, version):
        event_stream_id = f"{dataset_id}/{version}"
        event_stream = self.event_streams_table.get_event_stream(event_stream_id)

        if not event_stream:
            raise ResourceNotFound

        return event_stream.subscribable

    def enable_subscribable(self, dataset_id, version, updated_by):
        event_stream_id = f"{dataset_id}/{version}"
        event_stream = self.event_streams_table.get_event_stream(event_stream_id)

        if not event_stream or event_stream.deleted:
            raise ResourceNotFound
        if event_stream.subscribable.enabled:
            raise ResourceConflict

        event_stream.subscribable = Subscribable(
            cf_stack_template=generate_subscribable_cf_template(
                dataset_id=dataset_id,
                version=version,
                dataset_confidentiality=self.dataset_client.get_dataset(dataset_id)[
                    "confidentiality"
                ],
            ),
            cf_status="CREATE_IN_PROGRESS",
            enabled=True,
        )
        event_stream.config_version += 1
        event_stream.updated_by = updated_by
        event_stream.updated_at = datetime_utils.utc_now_with_timezone()

        self.event_streams_table.put_event_stream(event_stream)

        self.cloudformation_client.create_stack(
            name=generate_subscribable_cf_stack_name(dataset_id, version),
            template=event_stream.subscribable.cf_stack_template.json(),
            tags=[{"Key": "created_by", "Value": updated_by}],
        )

        return event_stream.subscribable

    def disable_subscribable(self, dataset_id, version, updated_by):
        event_stream_id = f"{dataset_id}/{version}"
        event_stream = self.event_streams_table.get_event_stream(event_stream_id)

        if not event_stream or event_stream.deleted:
            raise ResourceNotFound
        if not event_stream.subscribable.enabled:
            raise ResourceConflict

        event_stream.subscribable.cf_status = "DELETE_IN_PROGRESS"
        event_stream.subscribable.enabled = False
        event_stream.config_version += 1
        event_stream.updated_by = updated_by
        event_stream.updated_at = datetime_utils.utc_now_with_timezone()

        self.event_streams_table.put_event_stream(event_stream)

        self.cloudformation_client.delete_stack(
            name=generate_subscribable_cf_stack_name(dataset_id, version)
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


def generate_subscribable_cf_stack_name(dataset_id, version):
    return f"{CfStackType.SUBSCRIBABLE.value}-{dataset_id}-{version}"
