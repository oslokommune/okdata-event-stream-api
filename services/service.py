import json
import uuid

import boto3
import botocore
from okdata.aws.logging import log_add, log_duration
from okdata.sdk.data.dataset import Dataset

from clients import CloudformationClient
from database import EventStreamsTable, EventStream
from services import PutRecordsError, datetime_utils
from util import get_confidentiality

RETRY_CONFIG = botocore.config.Config(
    connect_timeout=3,
    read_timeout=3,
    retries={"max_attempts": 3, "mode": "standard"},
)


class EventService:
    def __init__(self, dataset_client: Dataset):
        self.dataset_client = dataset_client
        self.cloudformation_client = CloudformationClient()
        self.event_streams_table = EventStreamsTable()

    def get_event_stream(self, dataset_id, version):
        event_stream_id = f"{dataset_id}/{version}"
        return self.event_streams_table.get_event_stream(event_stream_id)

    def update_event_stream(self, event_stream: EventStream, updated_by: str):
        event_stream.config_version += 1
        event_stream.updated_by = updated_by
        event_stream.updated_at = datetime_utils.utc_now_with_timezone()
        self.event_streams_table.put_event_stream(event_stream)

    def send_events(self, dataset, version, events, retries=3):
        log_add(num_events=len(events))

        confidentiality = get_confidentiality(dataset)
        stream_name = self._stream_name(dataset["Id"], version, confidentiality)
        log_add(confidentiality=confidentiality, stream_name=stream_name)

        log_duration(
            lambda: self._put_records_to_kinesis(
                self._event_records(events), stream_name, retries
            ),
            "kinesis_put_records_duration",
        )

    def _stream_name(self, dataset_id, version, confidentiality):
        stage = "incoming"
        event_stream = log_duration(
            lambda: self.get_event_stream(dataset_id, version),
            "get_event_stream_duration",
        )
        if event_stream:
            stage = "raw"

        return f"dp.{confidentiality}.{dataset_id}.{stage}.{version}.json"

    def _event_records(self, events):
        return [
            {
                "Data": f"{json.dumps(event)}\n",
                "PartitionKey": str(uuid.uuid4()),
            }
            for event in events
        ]

    def _put_records_to_kinesis(self, records, stream_name, retries=3):
        client = boto3.client("kinesis", region_name="eu-west-1", config=RETRY_CONFIG)
        response = client.put_records(StreamName=stream_name, Records=records)

        if "Error" in response:
            log_add(kinesis_error=response["Error"])

        response_metadata = response["ResponseMetadata"]
        log_add(kinesis_retry_attempts=response_metadata.get("RetryAttempts"))
        log_add(kinesis_remaining_retries=retries)

        # Applying retry-strategy:
        # https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html
        if response["FailedRecordCount"] > 0:
            failed_records = self._failed_records(records, response["Records"])
            if retries > 0:
                return self._put_records_to_kinesis(
                    failed_records, stream_name, retries - 1
                )
            raise PutRecordsError(failed_records)

        return response

    def _failed_records(self, records, responses):
        return [
            record for i, record in enumerate(records) if "ErrorCode" in responses[i]
        ]
