import json

from services import ResourceConflict, EventStreamService
import pytest
import test.test_data.stream as test_data
from unittest.mock import ANY
from .conftest import username, valid_token


dataset_id = test_data.dataset_id
version = test_data.version
auth_header = {"Authorization": f"bearer {valid_token}"}
auth_header_invalid = {"Authorization": "bearer blablabla"}


def test_post_201(mock_client, mock_event_stream_service, mock_boto, mock_keycloak):
    response = mock_client.post(f"/{dataset_id}/{version}", headers=auth_header)

    EventStreamService.create_event_stream.assert_called_once_with(
        self=ANY,
        dataset_id=dataset_id,
        version=version,
        updated_by=username,
        create_raw=True,
    )

    assert response.status_code == 201
    assert json.loads(response.data) == json.loads(test_data.event_stream.json())


def test_post_not_create_raw(
    mock_client, mock_event_stream_service, mock_boto, mock_keycloak
):
    mock_client.post(
        f"/{dataset_id}/{version}", json={"create_raw": False}, headers=auth_header
    )
    EventStreamService.create_event_stream.assert_called_once_with(
        self=ANY,
        dataset_id=dataset_id,
        version=version,
        updated_by=username,
        create_raw=False,
    )


def test_post_401_invalid_token(mock_client, mock_keycloak, mock_boto):
    response = mock_client.post(
        f"/{dataset_id}/{version}", headers=auth_header_invalid,
    )
    assert response.status_code == 401
    assert json.loads(response.data) == {"message": "Invalid access token"}


def test_post_400_invalid_header_value(mock_client, mock_keycloak, mock_boto):
    response = mock_client.post(
        f"/{dataset_id}/{version}", headers={"Authorization": "blablabla"}
    )
    assert response.status_code == 400
    assert json.loads(response.data) == {
        "message": "Authorization header must match pattern: '^(b|B)earer [-0-9a-zA-Z\\._]*$'"
    }


def test_post_400_no_authorization_header(mock_client, mock_keycloak, mock_boto):
    response = mock_client.post(f"/{dataset_id}/{version}")
    assert response.status_code == 400
    assert json.loads(response.data) == {"message": "Missing authorization header"}


def test_post_409(
    mock_client, mock_event_stream_service_resource_conflict, mock_boto, mock_keycloak
):
    response = mock_client.post(
        f"/{dataset_id}/{version}", json={"create_raw": False}, headers=auth_header
    )
    assert response.status_code == 409
    assert json.loads(response.data) == {
        "message": f"Event stream with id {dataset_id}/{version} already exist"
    }


def test_post_500(
    mock_client, mock_event_stream_service_server_error, mock_boto, mock_keycloak
):
    response = mock_client.post(f"/{dataset_id}/{version}", headers=auth_header)
    assert response.status_code == 500
    assert json.loads(response.data) == {"message": "Server error"}


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
