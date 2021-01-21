import logging
import math

from elasticsearch_dsl import Search
from origo.data.dataset import Dataset

from database import ElasticsearchConnection
from util import CONFIDENTIALITY_MAP

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ElasticsearchDataService:
    def __init__(self, dataset_client: Dataset):
        self.dataset_client = dataset_client

    def get_event_by_date(
        self, dataset_id, version, from_date, to_date, page, page_size
    ):
        dataset = self.dataset_client.get_dataset(dataset_id)
        confidentiality = CONFIDENTIALITY_MAP[dataset["accessRights"]]
        index = f"processed-{confidentiality}-{dataset_id}-{version}-*"
        alias = "event_by_date"
        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)

        timestamp_field = dataset.get("timestamp_field", "timestamp")

        filter_args = {timestamp_field: {"gte": from_date, "lte": to_date}}
        s = Search(using=alias, index=index).filter("range", **filter_args)

        start_index = (page - 1) * page_size
        end_index = page * page_size

        s = s[start_index:end_index]

        logger.info(f"search: {s.to_dict()}")

        response = s.execute()

        if not response:
            logger.warning("Could not get a response from ES")
            return None

        return {
            "values": [e.to_dict() for e in s],
            "page": page,
            "total_pages": math.ceil(response.hits.total.value / page_size),
        }

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
