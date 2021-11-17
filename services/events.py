import json
import logging
import math
from typing import List

import boto3
from botocore.client import ClientError
from elasticsearch_dsl import Search
from okdata.sdk.data.dataset import Dataset

from database import ElasticsearchConnection
from services.exceptions import PostEventsError
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
        confidentiality = CONFIDENTIALITY_MAP[dataset["accessRights"]]
        index = f"processed-{confidentiality}-{dataset_id}-{version}-*"
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


class PostEventsService:
    def __init__(self, dataset_client: Dataset):
        self.kinesis_client = boto3.client("kinesis", region_name="eu-west-1")
        self.dataset_client: Dataset = dataset_client

    def post_events(self, dataset_id: str, version: str, events: List[dict]):

        stream_name = self.resolve_stream_name(dataset_id, version)

        logger.info(f"Posting {len(events)} events to {stream_name}")

        try:
            record_list = self.event_to_record_list(events)
            kinesis_response, failed_record_list = self.put_records_to_kinesis(
                record_list, stream_name
            )
        except ClientError as e:
            logger.error(e)
            raise PostEventsError

        if len(failed_record_list) > 0:
            # log_add(failed_records=len(failed_record_list))
            # return failed_elements_response(failed_record_list)
            # TODO: Return error response that will be translated to a 500 with body {"message": "bla bla", "failed_elements": [{}, {}]}
            raise Exception("Partial fail")

    def put_records_on_kinesis(self, record_list, stream_name, retries=3):
        put_records_response = self.kinesis_client.put_records(
            StreamName=stream_name, Records=record_list
        )

        # Applying retry-strategy: https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html
        if put_records_response["FailedRecordCount"] > 0:
            failed_record_list = self.get_failed_records(
                put_records_response, record_list
            )
            if retries > 0:
                return self.put_records_to_kinesis(
                    failed_record_list, stream_name, retries - 1
                )
            else:
                return put_records_response, failed_record_list
        else:
            return put_records_response, []

    def resolve_stream_name(self, dataset_id: str, version: str):
        access_rights = self.dataset_client.get_dataset(dataset_id)["accessRights"]
        confidentiality = CONFIDENTIALITY_MAP[access_rights]

        return f"dp.{confidentiality}.{dataset_id}.raw.{version}.json"

    @staticmethod
    def event_to_record_list(event_body):
        record_list = []

        for element in event_body:
            record_list.append(
                {"Data": f"{json.dumps(element)}\n", "PartitionKey": str(uuid.uuid4())}
            )

        return record_list

    @staticmethod
    def get_failed_records(put_records_response, record_list):
        failed_record_list = []
        for i in range(len(record_list)):
            if "ErrorCode" in put_records_response["Records"][i]:
                failed_record_list.append(record_list[i])
        return failed_record_list
