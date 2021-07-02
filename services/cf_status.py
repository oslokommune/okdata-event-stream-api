from database import EventStreamsTable, CfStackType


class CfStatusService:
    def __init__(self):
        self.event_streams_table = EventStreamsTable()

    def update_status(self, stack_name, cf_status):

        event_stream_id = resolve_event_stream_id(stack_name)

        event_stream = self.event_streams_table.get_event_stream(event_stream_id)

        cf_stack_type = resolve_cf_stack_type(stack_name)
        if cf_stack_type == CfStackType.EVENT_STREAM:
            event_stream.cf_status = cf_status
            self.event_streams_table.put_event_stream(event_stream)

        elif cf_stack_type == CfStackType.SUBSCRIBABLE:
            event_stream.subscribable.cf_status = cf_status
            self.event_streams_table.put_event_stream(event_stream)

        elif cf_stack_type == CfStackType.SINK:
            sink_id = stack_name.split("-")[-1]

            for sink in event_stream.sinks:
                if sink.id == sink_id:
                    sink.cf_status = cf_status

            self.event_streams_table.put_event_stream(event_stream)


def resolve_event_stream_id(stack_name):
    cf_stack_type = resolve_cf_stack_type(stack_name)

    stripped_prefix = stack_name.replace(f"{cf_stack_type.value}-", "", 1)
    components = stripped_prefix.split("-")

    if cf_stack_type in (CfStackType.EVENT_STREAM, CfStackType.SUBSCRIBABLE):
        dataset_id = "-".join(components[0:-1])
        version = components[-1]
        return f"{dataset_id}/{version}"
    elif cf_stack_type == CfStackType.SINK:
        dataset_id = "-".join(components[0:-2])
        version = components[-2]
        return f"{dataset_id}/{version}"
    return None


def resolve_cf_stack_type(stack_name):
    components = stack_name.split("-")
    return CfStackType(f"{components[0]}-{components[1]}")
