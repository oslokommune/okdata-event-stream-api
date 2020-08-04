import logging
import os

from elasticsearch_dsl import connections

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# the Elasticsearch API endpoint
path = os.environ["ES_API_ENDPOINT"]


class ElasticsearchConnection:
    def connect_to_es(self, index, alias):
        logger.warning("Connecting to ES")
        connections.create_connection(
            alias, hosts=[path], timeout=2,
        )
