import pytest

from database import ElasticsearchConnection
from elasticsearch_dsl import Search
from elasticsearch_dsl.response import Response
from origo.data.dataset import Dataset


@pytest.fixture
def mock_dataset(monkeypatch):
    def get_dataset(self, datasetid):
        return {"accessRights": "public"}

    monkeypatch.setattr(Dataset, "get_dataset", get_dataset)


@pytest.fixture
def mock_es_connection(monkeypatch):
    def connect_to_es(self, index, alias):
        pass

    monkeypatch.setattr(ElasticsearchConnection, "connect_to_es", connect_to_es)


@pytest.fixture
def mock_search(monkeypatch):
    def execute(self):
        response = {
            "hits": {"hits": [{"_source": {"foo": "bar"}}], "total": {"value": 1}}
        }
        return Response(search=self, response=response)

    monkeypatch.setattr(Search, "execute", execute)


@pytest.fixture
def mock_empty_search(monkeypatch):
    def execute(self):
        response = {"hits": {"hits": []}}
        return Response(search=self, response=response)

    monkeypatch.setattr(Search, "execute", execute)


@pytest.fixture
def mock_paging_search(monkeypatch, mocker):
    def execute(self):
        hits = [
            {"_source": {"a": 1}},
            {"_source": {"b": 2}},
            {"_source": {"c": 3}},
            {"_source": {"d": 4}},
            {"_source": {"e": 5}},
        ]
        response = {"hits": {"hits": hits, "total": {"value": 42}}}
        return Response(search=self, response=response)

    monkeypatch.setattr(Search, "execute", execute)
    mocker.spy(Search, "__getitem__")
