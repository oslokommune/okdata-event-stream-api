from origo.data.dataset import Dataset
from services.events import ElasticsearchDataService


def test_get_event_by_date(mock_dataset, mock_es_connection, mock_search):
    dataset_client = Dataset()
    service = ElasticsearchDataService(dataset_client)

    result = service.get_event_by_date(
        dataset_id="my-dataset",
        version="1",
        from_date="2020-01-01",
        to_date="2020-12-31",
    )

    assert len(result) == 1


def test_empty_search(mock_dataset, mock_es_connection, mock_empty_search):
    dataset_client = Dataset()
    service = ElasticsearchDataService(dataset_client)

    result = service.get_event_by_date(
        dataset_id="my-dataset",
        version="1",
        from_date="2020-01-01",
        to_date="2020-12-31",
    )

    assert result is None
