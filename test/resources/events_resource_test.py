import pytest
from services.events import ElasticsearchDataService

from .conftest import valid_token

event_dummy_data = {
    "tidspunkt": "2020-06-02T09:48:58+02:00",
    "plasseringId": 1,
    "sensorId": 1,
    "stasjonId": 41,
    "stasjon": "haraldrud-gjenbruk",
}
dataset_id = "besoksdata"
version = "1"
from_date = "2020-06-01"
to_date = "2020-06-05"

auth_header = {"Authorization": f"bearer {valid_token}"}


class TestGetEvent:
    def test_GetEventHistory(
        self,
        mock_client,
        mock_event_data,
        mock_dataset_versions,
        mock_authorizer,
        mock_keycloak,
    ):
        response = mock_client.get(
            f"/{dataset_id}/{version}/events?from_date={from_date}&to_date={to_date}",
            headers=auth_header,
        )
        response_data = response.get_json()
        assert response.status_code == 200
        assert response_data["stasjonId"] == 41

    def test_GetEventHistory_no_data(
        self,
        mock_client,
        mock_event_no_data,
        mock_dataset_versions,
        mock_authorizer,
        mock_keycloak,
    ):
        response = mock_client.get(
            f"/{dataset_id}/{version}/events?from_date={from_date}&to_date={to_date}",
            headers=auth_header,
        )
        response_data = response.get_json()
        assert response.status_code == 400
        assert (
            response_data["message"] == f"Could not found event: {dataset_id}/{version}"
        )


### Fixtures for TestGetEvent ###
@pytest.fixture()
def mock_event_data(monkeypatch):
    def get_event_by_date(self, dataset_id, version, from_date, to_date):
        return event_dummy_data

    monkeypatch.setattr(
        ElasticsearchDataService, "get_event_by_date", get_event_by_date,
    )


@pytest.fixture()
def mock_event_no_data(monkeypatch):
    def get_event_by_date(self, dataset_id, version, from_date, to_date):
        return None

    monkeypatch.setattr(
        ElasticsearchDataService, "get_event_by_date", get_event_by_date,
    )
