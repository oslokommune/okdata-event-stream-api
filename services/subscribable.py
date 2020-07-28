from database import EventStreamsTable
from services import ResourceNotFound


class SubscribableService:
    def __init__(self):
        self.event_streams_table = EventStreamsTable()

    def get_subscribable(self, dataset_id, version):
        event_stream_id = f"{dataset_id}/{version}"
        event_stream = self.event_streams_table.get_event_stream(event_stream_id)

        if not event_stream:
            raise ResourceNotFound

        return event_stream.subscribable
