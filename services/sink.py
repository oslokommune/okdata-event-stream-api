from database import EventStream, Sink, SinkType
from services import (
    EventService,
    ResourceNotFound,
    ResourceConflict,
    SubResourceNotFound,
    ResourceUnderConstruction,
    ResourceUnderDeletion,
    datetime_utils,
)
from services.template import SinkTemplate


class SinkService(EventService):
    def get_sinks(self, dataset_id: str, version: str) -> list:
        event_stream = self.get_event_stream(dataset_id, version)
        if not event_stream.sinks:
            return []
        return event_stream.sinks

    def get_sink_from_sink_list(self, sinks: list, sink_id) -> dict:
        for sink in sinks:
            if sink.id == sink_id and sink.deleted:
                raise SubResourceNotFound
            elif sink.id == sink_id:
                return sink
        raise SubResourceNotFound

    def get_sink(self, dataset_id: str, version: str, sink_id: str) -> dict:
        sinks = self.get_sinks(dataset_id, version)
        return self.get_sink_from_sink_list(sinks, sink_id)

    def get_sink_from_event_stream(
        self, event_stream: EventStream, sink_id: str
    ) -> dict:
        sinks = event_stream.sinks
        return self.get_sink_from_sink_list(sinks, sink_id)

    def check_for_existing_sink_type(
        self, event_stream: EventStream, sink_type: SinkType
    ) -> bool:
        for sink in event_stream.sinks:
            if sink.type == sink_type and sink.cf_status == "DELETE_IN_PROGRESS":
                raise ResourceUnderDeletion
            elif sink.type == sink_type.value and sink.deleted is False:
                raise ResourceConflict(
                    f"Sink: {sink_type.value} already exists on {event_stream.id}"
                )

    def enable_sink(
        self, dataset_id: str, version: str, sink_data: dict, updated_by: str
    ) -> Sink:
        event_stream = self.get_event_stream(dataset_id, version)
        if event_stream is None:
            raise ResourceNotFound
        if event_stream.deleted:
            raise ResourceNotFound

        sink_type = SinkType[sink_data["type"].upper()]
        self.check_for_existing_sink_type(event_stream, sink_type)

        dataset = self.dataset_client.get_dataset(dataset_id)

        sink = Sink(
            type=sink_type.value,
            cf_status="CREATE_IN_PROGRESS",
            updated_by=updated_by,
            updated_at=datetime_utils.utc_now_with_timezone(),
        )
        sink_template = SinkTemplate(event_stream, dataset, version, sink)
        sink.cf_stack_name = sink.get_stack_name(dataset_id, version)
        sink.cf_stack_template = sink_template.generate_stack_template()

        event_stream.sinks.append(sink)
        self.cloudformation_client.create_stack(
            name=sink.cf_stack_name,
            template=sink.cf_stack_template.json(),
            tags=[{"Key": "created_by", "Value": updated_by}],
        )
        self.update_event_stream(event_stream, updated_by)
        return sink

    def disable_sink(
        self, dataset_id: str, version: str, sink_id: str, updated_by: str
    ):
        event_stream = self.get_event_stream(dataset_id, version)
        if event_stream is None:
            raise ResourceNotFound
        if event_stream.deleted:
            raise ResourceNotFound

        sink = self.get_sink_from_event_stream(event_stream, sink_id)
        if sink.cf_status == "CREATE_IN_PROGRESS":
            raise ResourceUnderConstruction
        sink.cf_status = "DELETE_IN_PROGRESS"
        sink.deleted = True
        sink.updated_by = updated_by
        sink.updated_at = datetime_utils.utc_now_with_timezone()
        self.cloudformation_client.delete_stack(sink.cf_stack_name)
        self.update_event_stream(event_stream, updated_by)
