import json
import pytest
from freezegun import freeze_time
import dateutil.parser as date_parser

from origo.data.dataset import Dataset

from clients import setup_origo_sdk
from services import SubscribableService, ResourceNotFound, ResourceConflict
from services.subscribable import generate_subscribable_cf_template

from test import test_utils
import test.test_data.subscribable as test_data


def test_get_subscribable(mock_boto):
    event_stream = test_data.subscribable_event_stream
    test_utils.create_event_streams_table(item_list=[json.loads(event_stream.json())])

    subscribable_service = SubscribableService(
        setup_origo_sdk(test_data.ssm_parameters, Dataset)
    )

    subscribable = subscribable_service.get_subscribable(
        test_data.dataset_id, test_data.version
    )

    assert subscribable == event_stream.subscribable

    with pytest.raises(ResourceNotFound):
        subscribable_service.get_subscribable(test_data.dataset_id, 2)


@freeze_time(test_data.utc_now)
def test_enable_subscribable(mock_boto, mock_dataset):
    event_stream = test_data.event_stream
    test_utils.create_event_streams_table(item_list=[json.loads(event_stream.json())])

    subscribable_service = SubscribableService(
        setup_origo_sdk(test_data.ssm_parameters, Dataset)
    )

    subscribable = subscribable_service.enable_subscribable(
        test_data.dataset_id, test_data.version, test_data.updated_by
    )

    updated_event_stream = subscribable_service.event_streams_table.get_event_stream(
        event_stream.id
    )

    assert updated_event_stream.subscribable == subscribable
    assert updated_event_stream.subscribable.cf_status == "CREATE_IN_PROGRESS"
    assert updated_event_stream.config_version == event_stream.config_version + 1
    assert updated_event_stream.updated_by == test_data.updated_by
    assert updated_event_stream.updated_at == date_parser.parse(test_data.utc_now)

    with pytest.raises(ResourceNotFound):
        subscribable_service.enable_subscribable(
            test_data.dataset_id, 3, test_data.updated_by
        )

    with pytest.raises(ResourceConflict):
        subscribable_service.enable_subscribable(
            test_data.dataset_id, test_data.version, test_data.updated_by
        )


@freeze_time(test_data.utc_now)
def test_disable_subscribable(mock_boto, mock_dataset):
    event_stream = test_data.subscribable_event_stream
    test_utils.create_event_streams_table(item_list=[json.loads(event_stream.json())])

    subscribable_service = SubscribableService(
        setup_origo_sdk(test_data.ssm_parameters, Dataset)
    )

    subscribable = subscribable_service.disable_subscribable(
        test_data.dataset_id, test_data.version, test_data.updated_by
    )

    updated_event_stream = subscribable_service.event_streams_table.get_event_stream(
        event_stream.id
    )

    assert updated_event_stream.subscribable == subscribable
    assert updated_event_stream.subscribable.cf_status == "DELETE_IN_PROGRESS"
    assert updated_event_stream.config_version == event_stream.config_version + 1
    assert updated_event_stream.updated_by == test_data.updated_by
    assert updated_event_stream.updated_at == date_parser.parse(test_data.utc_now)

    with pytest.raises(ResourceNotFound):
        subscribable_service.disable_subscribable(
            test_data.dataset_id, 3, test_data.updated_by
        )

    with pytest.raises(ResourceConflict):
        subscribable_service.disable_subscribable(
            test_data.dataset_id, test_data.version, test_data.updated_by
        )


def test_generate_subscribable_cf_template():
    subscribable_template = generate_subscribable_cf_template(
        test_data.dataset_id, test_data.version, test_data.confidentiality
    )

    test_utils.validate_cf_template(subscribable_template.json())

    assert (
        json.loads(subscribable_template.json()) == test_data.subscribable_cf_template
    )


@pytest.fixture()
def mock_dataset(monkeypatch):
    def get_dataset(self, id):
        if id == test_data.dataset_id:
            return {"confidentiality": test_data.confidentiality}

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)
