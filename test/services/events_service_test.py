from unittest.mock import ANY

from elasticsearch_dsl import Search

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
        page=1,
        page_size=10,
    )

    assert len(result["values"]) == 1
    assert result["page"] == 1
    assert result["total_pages"] == 1


def test_empty_search(mock_dataset, mock_es_connection, mock_empty_search):
    dataset_client = Dataset()
    service = ElasticsearchDataService(dataset_client)

    result = service.get_event_by_date(
        dataset_id="my-dataset",
        version="1",
        from_date="2020-01-01",
        to_date="2020-12-31",
        page=1,
        page_size=10,
    )

    assert result is None


def test_paging(mock_dataset, mock_es_connection, mock_paging_search):
    dataset_client = Dataset()
    service = ElasticsearchDataService(dataset_client)

    result = service.get_event_by_date(
        dataset_id="my-dataset",
        version="1",
        from_date="2020-01-01",
        to_date="2020-12-31",
        page=3,
        page_size=5,
    )

    assert len(result["values"]) == 5
    assert result["page"] == 3
    assert result["total_pages"] == 9
    Search.__getitem__.assert_called_once_with(self=ANY, n=slice(10, 15))
