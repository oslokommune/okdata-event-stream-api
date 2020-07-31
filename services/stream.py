import os
import datetime
from origo.data.dataset import Dataset
from database import EventStreamsTable, EventStream, StackTemplate
from clients import CloudformationClient
from services import ResourceConflict


pipeline_router_lambda_name = f"pipeline-router-{os.environ['ORIGO_ENVIRONMENT']}-route"

# TODO: Enable/remove flag when https://jira.oslo.kommune.no/browse/DP-964 is done
create_pipeline_triggers = False


class EventStreamService:
    def __init__(self, dataset_client: Dataset):
        self.dataset_client = dataset_client
        self.cloudformation_client = CloudformationClient()
        self.event_streams_table = EventStreamsTable()

    def get_event_stream(self, dataset_id, version):
        event_stream_id = f"{dataset_id}/{version}"
        return self.event_streams_table.get_event_stream(event_stream_id)

    def create_event_stream(self, dataset_id, version, updated_by, create_raw=True):
        if self.get_event_stream(dataset_id, version):
            raise ResourceConflict

        event_stream_id = f"{dataset_id}/{version}"
        event_stream = EventStream(
            **{
                "id": event_stream_id,
                "create_raw": create_raw,
                "updated_by": updated_by,
                "updated_at": datetime.datetime.utcnow(),
                "cf_stack_template": generate_event_stream_cf_template(
                    dataset_id=dataset_id,
                    version=version,
                    dataset_confidentiality=self.dataset_client.get_dataset(dataset_id)[
                        "confidentiality"
                    ],
                    updated_by=updated_by,
                    create_raw=create_raw,
                ),
                "cf_status": "CREATE_IN_PROGRESS",
            }
        )
        self.event_streams_table.put_event_stream(event_stream)
        self.cloudformation_client.create_stack(
            name=event_stream.cf_stack_name,
            template=event_stream.cf_stack_template.json(),
            tags=[{"Key": "created_by", "Value": updated_by}],
        )
        return event_stream


def generate_event_stream_cf_template(
    dataset_id, version, dataset_confidentiality, updated_by, create_raw
):

    resources = {}

    if create_raw:
        raw_stream_name = (
            f"dp.{dataset_confidentiality}.{dataset_id}.raw.{version}.json"
        )
        resources["RawDataStream"] = data_stream_resource(raw_stream_name, updated_by)
        if create_pipeline_triggers:
            resources["RawPipelineTrigger"] = pipeline_trigger_resource(
                raw_stream_name, updated_by
            )

    processed_stream_name = (
        f"dp.{dataset_confidentiality}.{dataset_id}.processed.{version}.json"
    )
    resources["ProcessedDataStream"] = data_stream_resource(
        processed_stream_name, updated_by
    )
    if create_pipeline_triggers:
        resources["ProcessedPipelineTrigger"] = pipeline_trigger_resource(
            processed_stream_name, updated_by
        )

    return StackTemplate(
        **{
            "Description": f"Kinesis streams and pipeline triggers for {dataset_id}/{version}",
            "Resources": resources,
        }
    )


def data_stream_resource(stream_name, created_by):
    return {
        "Type": "AWS::Kinesis::Stream",
        "Properties": {
            "Name": stream_name,
            "ShardCount": 1,
            "Tags": [{"Key": "created_by", "Value": created_by}],
        },
    }


def pipeline_trigger_resource(stream_name, created_by):
    return {
        "Type": "AWS::Lambda::EventSourceMapping",
        "Properties": {
            "BatchSize": 10,
            "Enabled": True,
            "EventSourceArn": {
                "Fn::Sub": "arn:aws:kinesis:${AWS::Region}:${AWS::AccountId}:stream/"
                + f"{stream_name}"
            },
            "FunctionName": {
                "Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:"
                + pipeline_router_lambda_name
            },
            "StartingPosition": "LATEST",
        },
    }
