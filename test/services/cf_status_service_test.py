import json
from services.cf_status import resolve_cf_stack_type, resolve_event_stream_id
from services import CfStatusService
from database import CfStackType
from test import test_utils
import test.test_data.cf_status as test_data


def test_update_status(mock_boto):
    test_utils.create_event_streams_table(
        item_list=[json.loads(test_data.event_stream.json())]
    )

    cf_status_service = CfStatusService()

    # Test update event_stream cf_status

    cf_status_service.update_status(test_data.event_stream_stack_name, "ACTIVE")

    assert (
        cf_status_service.event_streams_table.get_event_stream(
            test_data.event_stream_id
        ).cf_status
        == "ACTIVE"
    )

    # Test update event subscribable cf_status

    cf_status_service.update_status(test_data.event_subscribable_stack_name, "ACTIVE")

    assert (
        cf_status_service.event_streams_table.get_event_stream(
            test_data.event_stream_id
        ).subscribable.cf_status
        == "ACTIVE"
    )

    # Test update event sink cf_status

    cf_status_service.update_status(test_data.event_sink_stack_name, "ACTIVE")

    event_sinks = cf_status_service.event_streams_table.get_event_stream(
        test_data.event_stream_id
    ).sinks

    [event_sink] = [
        event_sink for event_sink in event_sinks if event_sink.id == test_data.sink_id
    ]
    assert event_sink.cf_status == "ACTIVE"


def test_resolve_event_stream_id():

    assert (
        resolve_event_stream_id(test_data.event_stream_stack_name)
        == test_data.event_stream_id
    )
    assert (
        resolve_event_stream_id(test_data.event_subscribable_stack_name)
        == test_data.event_stream_id
    )
    assert (
        resolve_event_stream_id(test_data.event_sink_stack_name)
        == test_data.event_stream_id
    )


def test_resolve_cf_stack_type():
    assert (
        resolve_cf_stack_type(test_data.event_stream_stack_name)
        == CfStackType.EVENT_STREAM
    )
    assert (
        resolve_cf_stack_type(test_data.event_subscribable_stack_name)
        == CfStackType.SUBSCRIBABLE
    )
    assert resolve_cf_stack_type(test_data.event_sink_stack_name) == CfStackType.SINK
