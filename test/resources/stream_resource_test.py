import json

from services import ResourceConflict, EventStreamService, ResourceNotFound
import pytest
import test.test_data.stream as test_data
from unittest.mock import ANY
from .conftest import username, valid_token, valid_token_no_access


dataset_id = test_data.dataset_id
version = test_data.version
auth_header = {"Authorization": f"bearer {valid_token}"}


def test_post_201(
    mock_client, mock_event_stream_service, mock_keycloak, mock_authorizer
):
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
    mock_client, mock_event_stream_service, mock_keycloak, mock_authorizer
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


def test_post_401_invalid_token(mock_client, mock_keycloak, mock_authorizer):
    response = mock_client.post(
        f"/{dataset_id}/{version}", headers={"Authorization": "bearer blablabla"},
    )
    assert response.status_code == 401
    assert json.loads(response.data) == {"message": "Invalid access token"}


def test_post_400_invalid_header_value(mock_client, mock_keycloak, mock_authorizer):
    response = mock_client.post(
        f"/{dataset_id}/{version}", headers={"Authorization": "blablabla"}
    )
    assert response.status_code == 400
    assert json.loads(response.data) == {
        "message": "Authorization header must match pattern: '^(b|B)earer [-0-9a-zA-Z\\._]*$'"
    }


def test_post_400_no_authorization_header(mock_client, mock_keycloak, mock_authorizer):
    response = mock_client.post(f"/{dataset_id}/{version}")
    assert response.status_code == 400
    assert json.loads(response.data) == {"message": "Missing authorization header"}


def test_post_403(mock_client, mock_keycloak, mock_authorizer):
    response = mock_client.post(
        f"/{dataset_id}/{version}",
        headers={"Authorization": f"bearer {valid_token_no_access}"},
    )
    assert response.status_code == 403
    assert json.loads(response.data) == {"message": "Forbidden"}


def test_post_409(
    mock_client,
    mock_event_stream_service_resource_conflict,
    mock_keycloak,
    mock_authorizer,
):
    response = mock_client.post(
        f"/{dataset_id}/{version}", json={"create_raw": False}, headers=auth_header
    )
    assert response.status_code == 409
    assert json.loads(response.data) == {
        "message": f"Event stream with id {dataset_id}/{version} already exist"
    }


def test_post_500(
    mock_client, mock_event_stream_service_server_error, mock_keycloak, mock_authorizer,
):
    response = mock_client.post(f"/{dataset_id}/{version}", headers=auth_header)
    assert response.status_code == 500
    assert json.loads(response.data) == {"message": "Server error"}


def test_delete_200(
    mock_client, mock_keycloak, mock_authorizer, mock_event_stream_service
):
    response = mock_client.delete(
        f"/{dataset_id}/{version}", headers={"Authorization": f"bearer {valid_token}"},
    )
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "message": f"Deleted event stream with id {dataset_id}/{version}"
    }

    EventStreamService.delete_event_stream.assert_called_once_with(
        self=ANY, dataset_id=dataset_id, version=version, updated_by=username
    )


def test_delete_401(mock_client, mock_keycloak, mock_authorizer):
    response = mock_client.delete(
        f"/{dataset_id}/{version}", headers={"Authorization": "bearer blablabla"},
    )
    assert response.status_code == 401
    assert json.loads(response.data) == {"message": "Invalid access token"}


def test_delete_403(mock_client, mock_keycloak, mock_authorizer):
    response = mock_client.delete(
        f"/{dataset_id}/{version}",
        headers={"Authorization": f"bearer {valid_token_no_access}"},
    )
    assert response.status_code == 403
    assert json.loads(response.data) == {"message": "Forbidden"}


def test_delete_404_resource_not_found(
    mock_client,
    mock_keycloak,
    mock_authorizer,
    mock_event_stream_service_resource_not_found,
):
    response = mock_client.delete(
        f"/{dataset_id}/{version}", headers={"Authorization": f"bearer {valid_token}"},
    )
    assert response.status_code == 404
    assert json.loads(response.data) == {
        "message": f"Event stream with id {dataset_id}/{version} does not exist"
    }


def test_delete_409(
    mock_client,
    mock_keycloak,
    mock_authorizer,
    mock_event_stream_service_resource_conflict,
):
    response = mock_client.delete(
        f"/{dataset_id}/{version}", headers={"Authorization": f"bearer {valid_token}"},
    )
    assert response.status_code == 409
    assert json.loads(response.data) == {
        "message": f"Event stream with id {dataset_id}/{version} contains sub-resources. Delete all related event-sinks and disable event subscription"
    }


def test_delete_500(
    mock_client, mock_keycloak, mock_authorizer, mock_event_stream_service_server_error
):
    response = mock_client.delete(
        f"/{dataset_id}/{version}", headers={"Authorization": f"bearer {valid_token}"},
    )
    assert response.status_code == 500
    assert json.loads(response.data) == {"message": "Server error"}


@pytest.fixture()
def mock_event_stream_service(monkeypatch, mocker):
    def create_event_stream(self, dataset_id, version, updated_by, create_raw):
        return test_data.event_stream

    def delete_event_stream(self, dataset_id, version, updated_by):
        return

    monkeypatch.setattr(EventStreamService, "create_event_stream", create_event_stream)
    monkeypatch.setattr(EventStreamService, "delete_event_stream", delete_event_stream)

    mocker.spy(EventStreamService, "create_event_stream")
    mocker.spy(EventStreamService, "delete_event_stream")


@pytest.fixture()
def mock_event_stream_service_resource_conflict(monkeypatch):
    def create_event_stream(self, dataset_id, version, updated_by, create_raw):
        raise ResourceConflict

    def delete_event_stream(self, dataset_id, version, updated_by):
        raise ResourceConflict

    monkeypatch.setattr(EventStreamService, "create_event_stream", create_event_stream)
    monkeypatch.setattr(EventStreamService, "delete_event_stream", delete_event_stream)


@pytest.fixture()
def mock_event_stream_service_resource_not_found(monkeypatch):
    def delete_event_stream(self, dataset_id, version, updated_by):
        raise ResourceNotFound

    monkeypatch.setattr(EventStreamService, "delete_event_stream", delete_event_stream)


@pytest.fixture()
def mock_event_stream_service_server_error(monkeypatch):
    def create_event_stream(self, dataset_id, version, updated_by, create_raw):
        raise Exception

    def delete_event_stream(self, dataset_id, version, updated_by):
        raise Exception

    monkeypatch.setattr(EventStreamService, "create_event_stream", create_event_stream)
    monkeypatch.setattr(EventStreamService, "delete_event_stream", delete_event_stream)
