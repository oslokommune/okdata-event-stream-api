import pytest
from moto import mock_kinesis
from okdata.sdk.data.dataset import Dataset

from .conftest import valid_token, valid_token_no_access
from services import PutRecordsError
from services.service import EventService
from test.util import create_event_stream


def test_get_event_history(
    mock_client, mock_event_data, mock_dataset_versions, mock_authorizer, mock_keycloak
):
    response = mock_client.get(
        "/foo/1/events?from_date=2020-06-01&to_date=2020-06-05",
        headers={"Authorization": f"Bearer {valid_token}"},
    )
    data = response.json()
    assert response.status_code == 200
    assert data["stasjonId"] == 41


def test_get_event_history_no_data(
    mock_client,
    mock_event_no_data,
    mock_dataset_versions,
    mock_authorizer,
    mock_keycloak,
):
    response = mock_client.get(
        "/foo/1/events?from_date=2020-06-01&to_date=2020-06-05",
        headers={"Authorization": f"Bearer {valid_token}"},
    )
    data = response.json()
    assert response.status_code == 400
    assert data["message"] == "Could not find event: foo/1"


@mock_kinesis
def test_post_events(
    mock_authorizer,
    mock_client,
    mock_dataset,
    mock_dataset_versions,
    mock_keycloak,
    mock_stream_name,
):
    create_event_stream("dp.green.foo.incoming.1.json")
    res = mock_client.post(
        "/foo/1/events",
        headers={"Authorization": f"Bearer {valid_token}"},
        json=[{"foo": "bar"}],
    )
    assert res.status_code == 200


@mock_kinesis
def test_post_events_unauthorized(
    mock_authorizer,
    mock_client,
    mock_dataset,
    mock_dataset_versions,
    mock_keycloak,
    mock_stream_name,
):
    create_event_stream("dp.green.foo.incoming.1.json")
    res = mock_client.post(
        "/foo/1/events",
        headers={"Authorization": f"Bearer {valid_token_no_access}"},
        json={"events": [{"foo": "bar"}]},
    )
    assert res.status_code == 403


def test_post_events_failed_records(
    mock_authorizer,
    mock_client,
    mock_dataset,
    mock_dataset_versions,
    mock_keycloak,
    mock_stream_name,
    failed_records,
):
    res = mock_client.post(
        "/foo/1/events",
        headers={"Authorization": f"Bearer {valid_token}"},
        json=[{"foo": "bar"}],
    )
    assert res.status_code == 500
    assert res.json()["message"] == "Request failed for some elements: ['foo']"


@mock_kinesis
def test_post_events_validation_error(
    mock_authorizer,
    mock_client,
    mock_dataset,
    mock_dataset_versions,
    mock_keycloak,
    mock_stream_name,
):
    create_event_stream("dp.green.foo.incoming.1.json")
    res = mock_client.post(
        "/foo/1/events",
        headers={"Authorization": f"Bearer {valid_token}"},
        json=["invalid"],
    )
    assert res.status_code == 422


@pytest.fixture()
def failed_records(monkeypatch):
    def put_records_to_kinesis(*args, **kwargs):
        raise PutRecordsError(["foo"])

    monkeypatch.setattr(
        EventService,
        "_put_records_to_kinesis",
        put_records_to_kinesis,
    )


@pytest.fixture()
def mock_stream_name(monkeypatch):
    def stream_name(self, dataset_id, version, confidentiality):
        return f"dp.{confidentiality}.{dataset_id}.incoming.{version}.json"

    monkeypatch.setattr(EventService, "_stream_name", stream_name)


@pytest.fixture()
def mock_dataset(monkeypatch):
    def get_dataset(self, dataset_id, *args, **kwargs):
        if dataset_id == "foo":
            return {"Id": "foo", "accessRights": "public"}

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)
