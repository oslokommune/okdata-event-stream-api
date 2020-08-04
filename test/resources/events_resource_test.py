from services.events import ElasticsearchDataService

event_dummy_data = {
    "tidspunkt": "2020-06-02T09:48:58+02:00",
    "plasseringId": 1,
    "sensorId": 1,
    "stasjonId": 41,
    "stasjon": "haraldrud-gjenbruk",
}
dataset_id = "besoksdata"
version = "gjenbruksstasjoner"
from_date = "2020-06-01"
to_date = "2020-06-05"
doc_count = 10899


class TestGetEvent:
    def test_GetEventHistory(self, monkeypatch, mock_client):
        def get_event_history_data(self, dataset_id, version, from_date, to_date):
            return event_dummy_data

        monkeypatch.setattr(
            ElasticsearchDataService, "get_event_by_date", get_event_history_data,
        )
        response = mock_client.get(
            "/besoksdata/gjenbruksstasjoner/events?from_date=2020-06-01&to_date=2020-06-05"
        )
        response_data = response.get_json()
        print(response_data)
        assert response.status_code == 200
        assert response_data["stasjonId"] == 41

    def test_GetEventHistory_no_data(self, monkeypatch, mock_client):
        def get_event_history_data(self, dataset_id, version, from_date, to_date):
            return None

        monkeypatch.setattr(
            ElasticsearchDataService, "get_event_by_date", get_event_history_data,
        )
        response = mock_client.get(
            "/besoksdata/Noneexisting/events?from_date=2020-06-01&to_date=2020-06-05"
        )
        response_data = response.get_json()
        print(response_data)
        assert response.status_code == 400
        assert response_data["message"] == "No event found for provided id"
