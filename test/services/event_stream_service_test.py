import json
import pytest
from freezegun import freeze_time
from origo.data.dataset import Dataset
from services import EventStreamService, ResourceConflict
from services.stream import generate_event_stream_cf_template
import services.stream as stream
from clients import setup_origo_sdk

import test.test_data.stream as test_data

from test import test_utils


dataset_id = test_data.dataset_id
version = test_data.version
confidentiality = test_data.confidentiality
updated_by = test_data.updated_by

stream.create_pipeline_triggers = True


@freeze_time(test_data.utc_now)
def test_create_event_stream(mock_dataset, mock_boto):
    test_utils.create_event_streams_table()

    event_stream_service = EventStreamService(
        setup_origo_sdk(test_data.ssm_parameters, Dataset)
    )

    event_stream = event_stream_service.create_event_stream(
        dataset_id, version, updated_by, True
    )

    assert event_stream.id == f"{dataset_id}/{version}"
    assert (
        event_stream_service.event_streams_table.get_event_stream(event_stream.id)
        == event_stream
    )

    with pytest.raises(ResourceConflict):
        event_stream_service.create_event_stream(dataset_id, version, updated_by, True)


def test_generate_event_stream_cf_template():

    processed_and_raw_template = generate_event_stream_cf_template(
        dataset_id, version, confidentiality, updated_by, True
    )

    test_utils.validate_cf_template(processed_and_raw_template.json())
    assert (
        json.loads(processed_and_raw_template.json())
        == test_data.processed_and_raw_cf_template
    )

    processed_only_template = generate_event_stream_cf_template(
        dataset_id, version, confidentiality, updated_by, False
    )
    test_utils.validate_cf_template(processed_only_template.json())
    assert (
        json.loads(processed_only_template.json()) == test_data.processed_only_template
    )


@pytest.fixture()
def mock_dataset(monkeypatch):
    def get_dataset(self, id):
        if id == dataset_id:
            return {"confidentiality": test_data.confidentiality}

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)
