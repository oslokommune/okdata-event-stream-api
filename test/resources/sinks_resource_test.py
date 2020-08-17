import json
import pytest
from origo.data.dataset import Dataset

import test.test_data.stream as test_data
from .conftest import valid_token
from database import Sink
from database.models import EventStream
from services import EventStreamSinkService, SubResourceNotFound


dataset_id = test_data.dataset_id
version = test_data.version
sink_id = test_data.sink_id
auth_header = {"Authorization": f"bearer {valid_token}"}


class TestPostStreamSinkResource:
    def test_post_201(
        self,
        mock_client,
        mock_event_get_stream,
        mock_event_stream_service,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.post(
            f"/{dataset_id}/{version}/sinks", json={"type": "s3"}, headers=auth_header
        )
        data = json.loads(response.data)

        assert response.status_code == 201
        assert data["type"] == "s3"
        assert len(data["id"]) == 5

    def test_post_401_invalid_token(self, mock_client, mock_keycloak, mock_authorizer):
        response = mock_client.post(
            f"/{dataset_id}/{version}/sinks",
            headers={"Authorization": "bearer blablabla"},
        )
        assert response.status_code == 401
        assert json.loads(response.data) == {"message": "Invalid access token"}

    def test_post_400_invalid_header_value(
        self, mock_client, mock_keycloak, mock_authorizer
    ):
        response = mock_client.post(
            f"/{dataset_id}/{version}/sinks", headers={"Authorization": "blablabla"}
        )
        assert response.status_code == 400
        assert json.loads(response.data) == {
            "message": "Authorization header must match pattern: '^(b|B)earer [-0-9a-zA-Z\\._]*$'"
        }

    def test_post_400_no_authorization_header(
        self, mock_client, mock_keycloak, mock_authorizer
    ):
        response = mock_client.post(f"/{dataset_id}/{version}/sinks")
        assert response.status_code == 400
        assert json.loads(response.data) == {"message": "Missing authorization header"}

    def test_post_404_no_event_stream(
        self,
        mock_client,
        mock_event_stream_no_service,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.post(
            f"/{dataset_id}/{version}/sinks", json={"type": "s3"}, headers=auth_header
        )
        assert response.status_code == 404

    def test_post_400_invalid_sink_type(
        self,
        mock_client,
        mock_event_get_stream,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.post(
            f"/{dataset_id}/{version}/sinks",
            json={"type": "pompel"},
            headers=auth_header,
        )
        data = json.loads(response.data)
        assert response.status_code == 400
        assert data["message"] == "Invalid sink type: pompel"

    def test_post_400_sink_exists(
        self,
        mock_client,
        mock_event_get_stream_with_s3_sink,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.post(
            f"/{dataset_id}/{version}/sinks", json={"type": "s3"}, headers=auth_header,
        )
        data = json.loads(response.data)
        assert response.status_code == 409
        assert data["message"] == "Sink: s3 already exists on my-test-dataset"


class TestDeleteStreamSinkResource:
    def test_delete_404_resource_not_found(
        self,
        mock_client,
        mock_event_stream_no_service,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.delete(
            f"/{dataset_id}/{version}/sinks/{sink_id}", headers=auth_header
        )
        data = json.loads(response.data)
        assert response.status_code == 404
        assert (
            data["message"]
            == f"Event stream with id {dataset_id}/{version} does not exist"
        )

    def test_delete_404_sub_resource_not_found(
        self,
        mock_client,
        mock_event_get_stream,
        mock_event_sink_does_not_exist,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.delete(
            f"/{dataset_id}/{version}/sinks/{sink_id}", headers=auth_header
        )
        data = json.loads(response.data)
        assert response.status_code == 404
        assert (
            data["message"]
            == f"Sink with id {sink_id} does not exist on {dataset_id}/{version}"
        )


class TestGetStreamSinkResource:
    def test_get_200_sinks(
        self,
        mock_client,
        mock_event_get_stream,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.get(
            f"/{dataset_id}/{version}/sinks", headers=auth_header
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data) == 1

    def test_get_200_sink(
        self,
        mock_client,
        mock_event_get_stream,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.get(
            f"/{dataset_id}/{version}/sinks/fffff", headers=auth_header
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert data["id"] == "fffff"

    def test_get_404_sink(
        self,
        mock_client,
        mock_event_get_stream,
        mock_dataset_versions,
        mock_keycloak,
        mock_authorizer,
    ):
        response = mock_client.get(
            f"/{dataset_id}/{version}/sinks/pppppp", headers=auth_header
        )
        assert response.status_code == 404


@pytest.fixture()
def mock_event_stream_service(monkeypatch, mocker):
    def add_sink(self, dataset_id, version, request_data, updated_by):
        sink = Sink(type="s3", id="fffff")
        return sink

    monkeypatch.setattr(EventStreamSinkService, "add_sink", add_sink)


@pytest.fixture()
def mock_event_stream_no_service(monkeypatch):
    def get_event_stream(self, dataset_id, version):
        return None

    monkeypatch.setattr(EventStreamSinkService, "get_event_stream", get_event_stream)


@pytest.fixture()
def mock_event_sink_does_not_exist(monkeypatch):
    def get_sink(self, event_stream, sink_id):
        raise SubResourceNotFound

    monkeypatch.setattr(EventStreamSinkService, "get_sink", get_sink)


@pytest.fixture()
def mock_dataset_versions(monkeypatch):
    def get_versions(self, dataset_id):
        return [{"id": "my-test-dataset/1", "version": "1"}]

    monkeypatch.setattr(Dataset, "get_versions", get_versions)


@pytest.fixture()
def mock_event_get_stream_no_stream(monkeypatch):
    def get_event_stream(self, dataset_id, version):
        return None

    monkeypatch.setattr(EventStreamSinkService, "get_event_stream", get_event_stream)


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
            sinks=[
                {
                    "id": "fffff",
                    "type": "elasticsearch",
                    "cf_status": "ACTIVE",
                    "deleted": False,
                },
                {
                    "id": "gggg",
                    "type": "elasticsearch",
                    "cf_status": "DEACTIVE",
                    "deleted": True,
                },
            ],
        )

    monkeypatch.setattr(EventStreamSinkService, "get_event_stream", get_event_stream)

    def get_dataset(self, dataset_id):
        return {"Id": "my-test-dataset", "confidentiality": "green"}

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)


@pytest.fixture()
def mock_event_get_stream_with_s3_sink(monkeypatch):
    def get_event_stream(self, dataset_id, version):
        ret = EventStream(
            id="my-test-dataset",
            create_raw=True,
            updated_by="pompel",
            updated_at="2020-08-01T12:01:01",
            deleted=False,
            cf_status="ACTIVE",
        )
        sink = Sink(type="s3")
        ret.sinks.append(sink)
        return ret

    monkeypatch.setattr(EventStreamSinkService, "get_event_stream", get_event_stream)

    def get_dataset(self, dataset_id):
        return {"Id": "my-test-dataset", "confidentiality": "green"}

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)
