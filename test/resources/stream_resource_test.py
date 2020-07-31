import json
from origo.data.dataset import Dataset

from services import ResourceConflict, EventStreamService
import pytest
import test.test_data.stream as test_data
from unittest.mock import ANY
from .conftest import username, valid_token, valid_token_no_access
from database.models import EventStream


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


def test_post_403_invalid_token(mock_client, mock_keycloak, mock_authorizer):
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


def test_get_stream(
    mock_client,
    mock_event_get_stream,
    mock_dataset_versions,
    mock_keycloak,
    mock_authorizer,
):
    response = mock_client.get(f"/{dataset_id}/{version}", headers=auth_header)
    data = json.loads(response.data)
    assert response.status_code == 200
    assert data["id"] == "my-test-dataset"
    assert data["confidentiality"] == "green"


def test_get_stream_missing_dataset(
    mock_client,
    mock_event_get_stream_no_dataset,
    mock_dataset_versions,
    mock_keycloak,
    mock_authorizer,
):
    response = mock_client.get(f"/{dataset_id}/{version}", headers=auth_header)
    assert response.status_code == 404


def test_get_stream_missing_stream(
    mock_client,
    mock_event_get_stream_no_stream,
    mock_dataset_versions,
    mock_keycloak,
    mock_authorizer,
):
    response = mock_client.get(f"/{dataset_id}/{version}", headers=auth_header)
    assert response.status_code == 404


@pytest.fixture()
def mock_event_stream_service(monkeypatch, mocker):
    def create_event_stream(self, dataset_id, version, updated_by, create_raw):
        return test_data.event_stream

    monkeypatch.setattr(EventStreamService, "create_event_stream", create_event_stream)

    mocker.spy(EventStreamService, "create_event_stream")


@pytest.fixture()
def mock_dataset_versions(monkeypatch):
    def get_versions(self, dataset_id):
        return [{"id": "my-test-dataset/1", "version": "1"}]

    monkeypatch.setattr(Dataset, "get_versions", get_versions)


@pytest.fixture()
def mock_event_get_stream_no_stream(monkeypatch):
    def get_event_stream(self, dataset_id, version):
        return None

    monkeypatch.setattr(EventStreamService, "get_event_stream", get_event_stream)


@pytest.fixture()
def mock_event_get_stream_no_dataset(monkeypatch):
    def get_dataset(self, dataset_id):
        raise Exception("No such dataset")

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)


@pytest.fixture()
def mock_event_get_stream(monkeypatch):
    def get_event_stream(self, dataset_id, version):
        return EventStream(
            id="my-test-dataset",
            create_raw=True,
            updated_by="pompel",
            updated_at="2020-08-01T12:01:01",
            deleted=False,
            cf_status="ACTIVE",
        )

    monkeypatch.setattr(EventStreamService, "get_event_stream", get_event_stream)

    def get_dataset(self, dataset_id):
        return {"Id": "my-test-dataset", "confidentiality": "green"}

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)


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
