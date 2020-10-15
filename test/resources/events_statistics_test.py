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
doc_count = 10899

auth_header = {"Authorization": f"bearer {valid_token}"}


class TestStatistics:
    def test_get_event_count(
        self,
        mock_event_count,
        mock_client,
        mock_keycloak,
        mock_authorizer,
        mock_dataset_versions,
    ):
        response = mock_client.get(
            f"/{dataset_id}/{version}/events/statistics?from_date={from_date}&to_date={to_date}",
            headers=auth_header,
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == 10899

    def test_count_no_date(
        self,
        mock_client,
        mock_event_no_date,
        mock_keycloak,
        mock_authorizer,
        mock_dataset_versions,
    ):
        response = mock_client.get(
            f"/{dataset_id}/{version}/events/statistics", headers=auth_header
        )
        response_data = response.json()
        assert response.status_code == 422
        assert response_data["detail"] == [
            {
                "loc": ["query", "from_date"],
                "msg": "field required",
                "type": "value_error.missing",
            },
            {
                "loc": ["query", "to_date"],
                "msg": "field required",
                "type": "value_error.missing",
            },
        ]


### Fixtures for TestStatistics ###
@pytest.fixture()
def mock_event_count(monkeypatch):
    def get_event_count(self, dataset_id, version, from_date, to_date):
        return doc_count

    monkeypatch.setattr(ElasticsearchDataService, "get_event_count", get_event_count)


@pytest.fixture()
def mock_event_no_date(monkeypatch):
    def get_event_count(self, dataset_id, version, from_date, to_date):
        return None

    monkeypatch.setattr(ElasticsearchDataService, "get_event_count", get_event_count)
