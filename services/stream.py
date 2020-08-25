from database import EventStream
from services import (
    EventService,
    ResourceConflict,
    ResourceNotFound,
    datetime_utils,
)
from services.template import EventStreamTemplate


class EventStreamService(EventService):
    def create_event_stream(self, dataset_id, version, updated_by, create_raw=True):
        event_stream = self.get_event_stream(dataset_id, version)

        if event_stream is not None:
            if not event_stream.deleted:
                raise ResourceConflict

            event_stream.config_version += 1
            event_stream.deleted = False
            event_stream.updated_by = updated_by
            event_stream.updated_at = datetime_utils.utc_now_with_timezone()

        else:
            event_stream_id = f"{dataset_id}/{version}"
            event_stream = EventStream(
                **{
                    "id": event_stream_id,
                    "create_raw": create_raw,
                    "updated_by": updated_by,
                    "updated_at": datetime_utils.utc_now_with_timezone(),
                }
            )

        dataset = self.dataset_client.get_dataset(dataset_id)
        stream_template = EventStreamTemplate(dataset, version, updated_by, create_raw)
        event_stream.cf_stack_template = stream_template.generate_stack_template()
        event_stream.cf_status = "CREATE_IN_PROGRESS"
        event_stream.cf_stack_name = event_stream.get_stack_name()

        self.cloudformation_client.create_stack(
            name=event_stream.cf_stack_name,
            template=event_stream.cf_stack_template.json(),
            tags=[{"Key": "created_by", "Value": updated_by}],
        )
        self.event_streams_table.put_event_stream(event_stream)
        return event_stream

    def delete_event_stream(self, dataset_id, version, updated_by):
        event_stream = self.get_event_stream(dataset_id, version)

        if event_stream is None:
            raise ResourceNotFound
        if event_stream.deleted:
            raise ResourceNotFound

        if sub_resources_exist(event_stream):
            raise ResourceConflict

        event_stream.deleted = True
        event_stream.cf_status = "DELETE_IN_PROGRESS"
        self.cloudformation_client.delete_stack(event_stream.cf_stack_name)
        self.update_event_stream(event_stream, updated_by)


def sub_resources_exist(event_stream: EventStream):
    if event_stream.subscribable.cf_status != "INACTIVE":
        return True
    for sink in event_stream.sinks:
        if sink.cf_status != "INACTIVE":
            return True
    return False
