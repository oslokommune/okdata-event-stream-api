import logging

from origo.data.dataset import Dataset
from database import ElasticsearchConnection
from elasticsearch_dsl import Search

logger = logging.getLogger()


class ElasticsearchDataService:
    def __init__(self, dataset_client: Dataset):
        self.dataset_client = dataset_client

    def get_event_by_date(self, dataset_id, version, from_date, to_date):
        dataset = self.dataset_client.get_dataset(dataset_id)
        index = f'processed-{dataset["confidentiality"]}-{dataset_id}-{version}-*'
        alias = "event_by_date"
        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)

        timestamp_field = dataset.get("timestamp_field", "timestamp")

        filter_args = {timestamp_field: {"gte": from_date, "lte": to_date}}
        s = Search(using=alias, index=index).filter("range", **filter_args)

        response = s.execute()

        if not response:
            logger.warning("Could not get a response from ES")
            return None
        return [e.to_dict() for e in s]

    def get_event_count(self, dataset_id, version, from_date, to_date):
        dataset = self.dataset_client.get_dataset(dataset_id)
        index = f"processed-{dataset['confidentiality']}-{dataset_id}-{version}-*"
        alias = "event_by_count"
        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)
        timestamp_field = dataset.get("timestamp_field", "timestamp")
        body = {
            "size": 0,
            "aggs": {
                "count_events": {
                    "range": {
                        "field": timestamp_field,
                        "ranges": [{"from": from_date, "to": to_date}],
                    }
                }
            },
        }
        s = Search(using=alias, index=index).update_from_dict(body)
        response = s.execute()

        count = response["aggregations"]["count_events"]["buckets"][0]["doc_count"]

        return count
