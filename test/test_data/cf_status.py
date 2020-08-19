from database import EventStream


dataset_id = "dataset-id"
version = "version"
sink_id = "0agee"
event_stream_id = f"{dataset_id}/{version}"

event_stream_stack_name = f"event-stream-{dataset_id}-{version}"
event_subscribable_stack_name = f"event-subscribable-{dataset_id}-{version}"
event_sink_stack_name = f"event-sink-{dataset_id}-{version}-{sink_id}"

cf_stack_template = {
    "description": "foo",
    "resources": {"foo": {"type": "bar", "properties": {"foo": "bar"}}},
}

event_stream = EventStream(
    **{
        "cf_stack_template": cf_stack_template,
        "cf_status": "CREATE_IN_PROGRESS",
        "cf_stack_name": f"event-stream-{dataset_id}-{version}",
        "id": event_stream_id,
        "create_raw": True,
        "updated_by": "larsmonsen",
        "updated_at": "2020-01-21T09:28:57.831435",
        "deleted": False,
        "subscribable": {
            "enabled": True,
            "cf_stack_template": cf_stack_template,
            "cf_stack_name": f"event-subscribable-{dataset_id}-{version}",
            "cf_status": "CREATE_IN_PROGRESS",
        },
        "sinks": [
            {
                "id": "c8sh5",
                "type": "s3",
                "config": {"write_interval_seconds": 300},
                "cf_stack_template": cf_stack_template,
                "cf_stack_name": f"event-sink-{dataset_id}-{version}-c8sh5",
                "cf_status": "ACTIVE",
            },
            {
                "id": sink_id,
                "type": "elasticsearch",
                "config": {"es_cluster": "some-uri"},
                "cf_stack_template": cf_stack_template,
                "cf_stack_name": f"event-sink-{dataset_id}-{version}-{sink_id}",
                "cf_status": "CREATE_IN_PROGRESS",
            },
        ],
    }
)
