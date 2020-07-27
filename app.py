from flask import Flask
from flask_restful import Api

from origo.dataset_authorizer.simple_dataset_authorizer_client import (
    SimpleDatasetAuthorizerClient,
)
from origo.data.dataset import Dataset
from clients import setup_keycloak_client, setup_origo_sdk, get_keycloak_config
from resources.routes import initialize_routes


app = Flask(__name__)
app.url_map.strict_slashes = False
api = Api(app)

keycloak_config = get_keycloak_config()
app.keycloak_client = setup_keycloak_client(keycloak_config)
app.simple_dataset_authorizer_client = setup_origo_sdk(
    keycloak_config, SimpleDatasetAuthorizerClient
)
app.dataset_client = setup_origo_sdk(keycloak_config, Dataset)

initialize_routes(api)
