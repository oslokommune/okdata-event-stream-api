from okdata.sdk.data.dataset import Dataset

from database import EventStreamsTable, EventStream
from clients import CloudformationClient
from services import datetime_utils


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
