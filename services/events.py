import logging
import math

from elasticsearch_dsl import Search
from okdata.sdk.data.dataset import Dataset

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
        dataset, index = self.get_es_information(dataset_id, version)
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
        dataset, index = self.get_es_information(dataset_id, version)
        alias = "event_by_count"
        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)
        timestamp_field = dataset.get("timestamp_field", "timestamp")
        body = {
            "size": 0,
            "aggs": {
                "count_events": {
                    "date_range": {
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

    def get_event_count_granular(
        self, dataset_id, version, from_range, to_range, pattern
    ):
        dataset, index = self.get_es_information(dataset_id, version)
        alias = "event_by_count"
        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)
        s = Search(using=alias, index=index)

        s.aggs.bucket(
            "count_events",
            "date_range",
            field="timestamp",
            ranges=[{"to": f"{to_range}/{pattern}", "from": f"{from_range}/{pattern}"}],
        )

        response = s.execute()

        count = response["aggregations"]["count_events"]["buckets"][0]["doc_count"]

        return count

    def get_single_aggregation(
        self, dataset_id, version, from_date, to_date, field, size
    ):
        dataset, index = self.get_es_information(dataset_id, version)
        alias = "single_aggregation"

        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)

        timestamp_field = dataset.get("timestamp_field", "timestamp")

        filter_args = {timestamp_field: {"gte": from_date, "lte": to_date}}

        s = Search(using=alias, index=index).filter("range", **filter_args)

        s.aggs.bucket(
            f"count_per_{field}", "terms", field=f"{field}.keyword", size=size
        )

        response = s.execute()

        bucket = response["aggregations"][f"count_per_{field}"]["buckets"]

        return {"data": [x.to_dict() for x in bucket]}

    def count_events_by_range(
        self, dataset_id, version, from_date, to_date, from_range, to_range
    ):
        dataset, index = self.get_es_information(dataset_id, version)
        alias = "count_events_by_range"

        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)

        timestamp_field = dataset.get("timestamp_field", "timestamp")

        filter_args = {timestamp_field: {"gte": from_date, "lte": to_date}}

        s = Search(using=alias, index=index).filter("range", **filter_args)

        s.aggs.bucket(
            "count",
            "date_range",
            field="timestamp",
            ranges=[{"to": to_range, "from": from_range}],
        )

        response = s.execute()

        return response["aggregations"]["count"]["buckets"]

    def get_histogram(self, dataset_id, version, from_date, to_date, interval):
        dataset, index = self.get_es_information(dataset_id, version)
        alias = "events_by_count_per_day"

        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)

        timestamp_field = dataset.get("timestamp_field", "timestamp")

        filter_args = {timestamp_field: {"gte": from_date, "lte": to_date}}

        timestamp_field = dataset.get("timestamp_field", "timestamp")

        s = Search(using=alias, index=index).filter("range", **filter_args)

        s.aggs.bucket(
            f"call_per_{interval}",
            "date_histogram",
            field=timestamp_field,
            interval=interval,
        )

        response = s.execute()

        bucket = response["aggregations"][f"call_per_{interval}"]["buckets"]

        return {"values": [x.to_dict() for x in bucket]}

    def get_average_count(self, dataset_id, version, field, interval):
        dataset, index = self.get_es_information(dataset_id, version)
        alias = f"average_count_by_{interval}"

        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)

        s = Search(using=alias, index=index)

        s.aggs.bucket(
            f"count_per_{interval}", "date_histogram", field=field, interval=interval
        )
        s.aggs.bucket(
            f"avg_count_per_{interval}",
            "avg_bucket",
            buckets_path=f"count_per_{interval}>_count",
        )

        response = s.execute()

        return response["aggregations"][f"avg_count_per_{interval}"]["value"]

    def get_es_information(self, dataset_id, version):
        dataset = self.dataset_client.get_dataset(dataset_id)
        confidentiality = CONFIDENTIALITY_MAP[dataset["accessRights"]]
        index = f"processed-{confidentiality}-{dataset_id}-{version}-*"

        return dataset, index
