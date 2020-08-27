import logging
import os

from elasticsearch import RequestsHttpConnection
from elasticsearch_dsl import connections
from requests_aws4auth import AWS4Auth
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# the Elasticsearch API endpoint
path = os.environ["ES_API_ENDPOINT"]
region = os.environ["AWS_REGION"]


class ElasticsearchConnection:
    def connect_to_es(self, index, alias):
        logger.info(f"Connecting to ES: {path}")
        try:
            service = "es"
            credentials = boto3.Session().get_credentials()
            awsauth = AWS4Auth(
                credentials.access_key,
                credentials.secret_key,
                region,
                service,
                session_token=credentials.token,
            )

            connections.create_connection(
                alias,
                hosts=[{"host": path, "scheme": "https", "port": 443}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=2,
            )
            logger.info("Connected to ES")
        except Exception as e:
            logger.error(f"Unable to connect to ES: {str(e)}")
