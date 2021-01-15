import pytest
import dateutil.parser as date_parser
from freezegun import freeze_time
from origo.data.dataset import Dataset
from services import EventStreamService, ResourceConflict, ResourceNotFound

from clients import setup_origo_sdk, CloudformationClient
from unittest.mock import ANY

import test.test_data.stream as test_data

from test import test_utils


@freeze_time(test_data.created_at)
def test_create_event_stream(mock_dataset, mock_boto, mocker):

    mocker.spy(CloudformationClient, "create_stack")

    test_utils.create_event_streams_table()

    event_stream_service = EventStreamService(
        setup_origo_sdk(test_data.ssm_parameters, Dataset)
    )

    event_stream = event_stream_service.create_event_stream(
        test_data.dataset_id, test_data.version, test_data.updated_by, True
    )

    CloudformationClient.create_stack.assert_called_once_with(
        self=ANY,
        name=event_stream.cf_stack_name,
        template=event_stream.cf_stack_template.json(),
        tags=[{"Key": "created_by", "Value": event_stream.updated_by}],
    )

    assert event_stream.id == f"{test_data.dataset_id}/{test_data.version}"
    assert (
        event_stream_service.event_streams_table.get_event_stream(event_stream.id)
        == event_stream
    )

    with pytest.raises(ResourceConflict):
        event_stream_service.create_event_stream(
            test_data.dataset_id, test_data.version, test_data.updated_by, True
        )

    event_stream_service.event_streams_table.put_event_stream(
        test_data.deleted_event_stream
    )

    event_stream = event_stream_service.create_event_stream(
        test_data.dataset_id, test_data.version, test_data.updated_by
    )

    assert (
        event_stream.config_version == test_data.deleted_event_stream.config_version + 1
    )


@freeze_time(test_data.deleted_at)
def test_delete_event_stream(mock_boto, mocker):

    mocker.spy(CloudformationClient, "delete_stack")

    test_utils.create_event_streams_table()

    event_stream_service = EventStreamService(
        setup_origo_sdk(test_data.ssm_parameters, Dataset)
    )

    with pytest.raises(ResourceNotFound):
        event_stream_service.delete_event_stream(
            test_data.dataset_id, test_data.version, test_data.updated_by
        )

    event_stream_service.event_streams_table.put_event_stream(test_data.event_stream)

    event_stream_service.delete_event_stream(
        test_data.dataset_id, test_data.version, test_data.updated_by
    )

    deleted_event_stream = event_stream_service.event_streams_table.get_event_stream(
        test_data.event_stream.id
    )

    CloudformationClient.delete_stack.assert_called_once_with(
        self=ANY, name=deleted_event_stream.cf_stack_name
    )

    assert deleted_event_stream.deleted
    assert deleted_event_stream.config_version == 2
    assert deleted_event_stream.updated_by == test_data.updated_by
    assert deleted_event_stream.cf_status == "DELETE_IN_PROGRESS"
    assert deleted_event_stream.updated_at == date_parser.parse(test_data.deleted_at)

    with pytest.raises(ResourceNotFound):
        event_stream_service.delete_event_stream(
            test_data.dataset_id, test_data.version, test_data.updated_by
        )


def test_delete_fails_if_subresources_exist(mock_boto):
    test_utils.create_event_streams_table()

    event_stream_service = EventStreamService(
        setup_origo_sdk(test_data.ssm_parameters, Dataset)
    )

    event_stream_service.event_streams_table.put_event_stream(
        test_data.event_stream_with_subresources
    )

    with pytest.raises(ResourceConflict):
        event_stream_service.delete_event_stream(
            test_data.dataset_id, test_data.version, test_data.updated_by
        )


@pytest.fixture()
def mock_dataset(monkeypatch):
    def get_dataset(self, id):
        if id == test_data.dataset_id:
            return {"Id": id, "accessRights": test_data.accessRights}

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)
