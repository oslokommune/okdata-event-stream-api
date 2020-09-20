from origo.data.dataset import Dataset

from services import ResourceConflict, EventStreamService, ResourceNotFound
import pytest
import test.test_data.stream as test_data
from unittest.mock import ANY
from .conftest import username, valid_token, valid_token_no_access
from database.models import EventStream


dataset_id = test_data.dataset_id
version = test_data.version
auth_header = {"Authorization": f"bearer {valid_token}"}


class TestPostStreamResource:
    def test_post_201(
        self, mock_client, mock_event_stream_service, mock_keycloak, mock_authorizer
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
        assert response.json() == {
            "id": test_data.event_stream.id,
            "create_raw": test_data.event_stream.create_raw,
            "updated_by": test_data.event_stream.updated_by,
            "updated_at": test_data.event_stream.updated_at.isoformat(),
            "deleted": test_data.event_stream.deleted,
            "status": test_data.event_stream.cf_status,
        }

    def test_post_not_create_raw(
        self, mock_client, mock_event_stream_service, mock_keycloak, mock_authorizer
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

    def test_post_401_invalid_token(self, mock_client, mock_keycloak, mock_authorizer):
        response = mock_client.post(
            f"/{dataset_id}/{version}",
            headers={"Authorization": "bearer blablabla"},
        )
        assert response.status_code == 401
        assert response.json() == {"message": "Invalid access token"}

    def test_post_invalid_header_value(
        self, mock_client, mock_keycloak, mock_authorizer
    ):
        response = mock_client.post(
            f"/{dataset_id}/{version}", headers={"Authorization": "blablabla"}
        )
        assert response.status_code == 403
        assert response.json() == {"detail": "Not authenticated"}

    def test_post_400_no_authorization_header(
        self, mock_client, mock_keycloak, mock_authorizer
    ):
        response = mock_client.post(f"/{dataset_id}/{version}")
        assert response.status_code == 403
        assert response.json() == {"detail": "Not authenticated"}

    def test_post_403(self, mock_client, mock_keycloak, mock_authorizer):
        response = mock_client.post(
            f"/{dataset_id}/{version}",
            headers={"Authorization": f"bearer {valid_token_no_access}"},
        )
        assert response.status_code == 403
        assert response.json() == {"message": "Forbidden"}

    def test_post_409(
        self,
        mock_client,
        mock_event_stream_service_resource_conflict,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.post(
            f"/{dataset_id}/{version}", json={"create_raw": False}, headers=auth_header
        )
        assert response.status_code == 409
        assert response.json() == {
            "message": f"Event stream with id {dataset_id}/{version} already exist"
        }

    def test_post_500(
        self,
        mock_client,
        mock_event_stream_service_server_error,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.post(f"/{dataset_id}/{version}", headers=auth_header)
        assert response.status_code == 500
        assert response.json() == {"message": "Server error"}


class TestDeleteStreamResource:
    def test_delete_200(
        self,
        mock_client,
        mock_keycloak,
        mock_authorizer,
        mock_event_stream_service,
        mock_dataset_versions,
    ):
        response = mock_client.delete(
            f"/{dataset_id}/{version}",
            headers={"Authorization": f"bearer {valid_token}"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "message": f"Deleted event stream with id {dataset_id}/{version}"
        }

        EventStreamService.delete_event_stream.assert_called_once_with(
            self=ANY, dataset_id=dataset_id, version=version, updated_by=username
        )

    def test_delete_401(self, mock_client, mock_keycloak, mock_authorizer):
        response = mock_client.delete(
            f"/{dataset_id}/{version}",
            headers={"Authorization": "bearer blablabla"},
        )
        assert response.status_code == 401
        assert response.json() == {"message": "Invalid access token"}

    def test_delete_403(self, mock_client, mock_keycloak, mock_authorizer):
        response = mock_client.delete(
            f"/{dataset_id}/{version}",
            headers={"Authorization": f"bearer {valid_token_no_access}"},
        )
        assert response.status_code == 403
        assert response.json() == {"message": "Forbidden"}

    def test_delete_404_resource_not_found(
        self,
        mock_client,
        mock_keycloak,
        mock_authorizer,
        mock_dataset_versions,
        mock_event_stream_service_resource_not_found,
    ):
        response = mock_client.delete(
            f"/{dataset_id}/{version}",
            headers={"Authorization": f"bearer {valid_token}"},
        )
        assert response.status_code == 404
        assert response.json() == {
            "message": f"Event stream with id {dataset_id}/{version} does not exist"
        }

    def test_delete_409(
        self,
        mock_client,
        mock_keycloak,
        mock_authorizer,
        mock_dataset_versions,
        mock_event_stream_service_resource_conflict,
    ):
        response = mock_client.delete(
            f"/{dataset_id}/{version}",
            headers={"Authorization": f"bearer {valid_token}"},
        )
        assert response.status_code == 409
        assert response.json() == {
            "message": f"Event stream with id {dataset_id}/{version} contains sub-resources. Delete all related event-sinks and disable event subscription"
        }

    def test_delete_500(
        self,
        mock_client,
        mock_keycloak,
        mock_authorizer,
        mock_dataset_versions,
        mock_event_stream_service_server_error,
    ):
        response = mock_client.delete(
            f"/{dataset_id}/{version}",
            headers={"Authorization": f"bearer {valid_token}"},
        )
        assert response.status_code == 500
        assert response.json() == {"message": "Server error"}


class TestGetStreamResource:
    def test_get_200(
        self,
        mock_client,
        mock_event_get_stream,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.get(f"/{dataset_id}/{version}", headers=auth_header)
        data = response.json()
        assert response.status_code == 200
        assert data["id"] == "my-test-dataset"
        assert data["confidentiality"] == "green"

    def test_get_403_no_access(self, mock_client, mock_keycloak, mock_authorizer):
        response = mock_client.get(
            f"/{dataset_id}/{version}",
            headers={"Authorization": f"bearer {valid_token_no_access}"},
        )
        assert response.status_code == 403
        assert response.json() == {"message": "Forbidden"}

    def test_get_404_version_missing(
        self, mock_client, mock_dataset_versions, mock_keycloak, mock_authorizer
    ):
        response = mock_client.get(f"/{dataset_id}/99999", headers=auth_header)
        assert response.status_code == 404

    def test_get_404_missing_dataset(
        self,
        mock_client,
        mock_event_get_stream_no_dataset,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.get(f"/{dataset_id}/{version}", headers=auth_header)
        assert response.status_code == 404

    def test_get_404_missing_stream(
        self,
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


### Fixtures for TestGetStreamResource ###
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
