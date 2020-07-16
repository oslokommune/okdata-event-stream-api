import json

from services import ResourceConflict, EventStreamService
import pytest
from app import app as flask_app
import test.test_data.stream as test_data
from unittest.mock import ANY

dataset_id = test_data.dataset_id
version = test_data.version


def test_post_201(mock_client, mock_event_stream_service, mock_boto):
    response = mock_client.post(f"/{dataset_id}/{version}")

    EventStreamService.create_event_stream.assert_called_once_with(
        self=ANY,
        dataset_id=dataset_id,
        version=version,
        updated_by="janedone",
        create_raw=True,
    )

    assert response.status_code == 201
    assert json.loads(response.data) == json.loads(test_data.event_stream.json())


def test_post_not_create_raw(mock_client, mock_event_stream_service, mock_boto):
    mock_client.post(f"/{dataset_id}/{version}", json={"create_raw": False})
    EventStreamService.create_event_stream.assert_called_once_with(
        self=ANY,
        dataset_id=dataset_id,
        version=version,
        updated_by="janedone",
        create_raw=False,
    )


def test_post_409(mock_client, mock_event_stream_service_resource_conflict, mock_boto):
    response = mock_client.post(f"/{dataset_id}/{version}", json={"create_raw": False})
    assert response.status_code == 409
    assert json.loads(response.data) == {
        "message": f"Event stream with id {dataset_id}/{version} already exist"
    }


def test_post_500(mock_client, mock_event_stream_service_server_error, mock_boto):
    response = mock_client.post(f"/{dataset_id}/{version}", json={"create_raw": False})
    assert response.status_code == 500
    assert json.loads(response.data) == {"message": "Server error"}


@pytest.fixture
def mock_client():
    # Configure the application for testing and disable error catching during
    # request handling for better reports. Required in order for exceptions
    # to propagate to the test client.
    # https://flask.palletsprojects.com/en/1.1.x/testing/
    # https://flask.palletsprojects.com/en/1.1.x/api/#flask.Flask.test_client
    flask_app.config["TESTING"] = True

    with flask_app.test_client() as client:
        yield client


@pytest.fixture()
def mock_event_stream_service(monkeypatch, mocker):
    def create_event_stream(self, dataset_id, version, updated_by, create_raw):
        return test_data.event_stream

    monkeypatch.setattr(EventStreamService, "create_event_stream", create_event_stream)

    mocker.spy(EventStreamService, "create_event_stream")


@pytest.fixture()
def mock_event_stream_service_resource_conflict(monkeypatch, mocker):
    def create_event_stream(self, dataset_id, version, updated_by, create_raw):
        raise ResourceConflict()

    monkeypatch.setattr(EventStreamService, "create_event_stream", create_event_stream)


@pytest.fixture()
def mock_event_stream_service_server_error(monkeypatch, mocker):
    def create_event_stream(self, dataset_id, version, updated_by, create_raw):
        raise Exception

    monkeypatch.setattr(EventStreamService, "create_event_stream", create_event_stream)
