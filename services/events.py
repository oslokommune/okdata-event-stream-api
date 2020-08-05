import logging
from database import ElasticsearchConnection
from elasticsearch_dsl import Search

logger = logging.getLogger()


class ElasticsearchDataService:
    def get_event_by_date(self, dataset_id, version, from_date, to_date):
        index = dataset_id + "-" + version + "-" + "*"
        alias = "event_by_date"
        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)
        s = Search(using=alias, index=index).filter(
            "range", tidspunkt={"gte": from_date, "lt": to_date}
        )
        response = s.execute()

        if not response:
            logger.warning("Could not get a response from ES")
            return None
        return [e.to_dict() for e in s]

    def get_event_count(self, dataset_id, version, from_date, to_date):
        index = dataset_id + "-" + version + "-" + "*"
        alias = "event_by_count"
        es = ElasticsearchConnection()
        es.connect_to_es(index, alias)
        body = {
            "size": 0,
            "aggs": {
                "count_events": {
                    "range": {
                        "field": "tidspunkt",
                        "ranges": [{"from": from_date, "to": to_date}],
                    }
                }
            },
        }
        s = Search(using=alias, index=index).update_from_dict(body)
        response = s.execute()

        count = response["aggregations"]["count_events"]["buckets"][0]["doc_count"]

        return count
