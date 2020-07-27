from resources import StreamResource, SubscribableResource, SinkResource, SinksResource
from clients import setup_keycloak_client, setup_origo_sdk, get_keycloak_config
from origo.dataset_authorizer.simple_dataset_authorizer_client import (
    SimpleDatasetAuthorizerClient,
)
from origo.data.dataset import Dataset


def initialize_routes(api):

    keycloak_config = get_keycloak_config()
    keycloak_client = setup_keycloak_client(keycloak_config)

    simple_dataset_authorizer_client = setup_origo_sdk(
        keycloak_config, SimpleDatasetAuthorizerClient
    )
    dataset_client = setup_origo_sdk(keycloak_config, Dataset)

    api.add_resource(
        StreamResource,
        "/<string:dataset_id>/<string:version>",
        resource_class_args=(
            keycloak_client,
            simple_dataset_authorizer_client,
            dataset_client,
        ),
    )
    api.add_resource(
        SinksResource,
        "/<string:dataset_id>/<string:version>/sinks",
        resource_class_args=(keycloak_client, simple_dataset_authorizer_client),
    )
    api.add_resource(
        SinkResource,
        "/<string:dataset_id>/<string:version>/sinks/<string:sink_id>",
        resource_class_args=(keycloak_client, simple_dataset_authorizer_client),
    )
    api.add_resource(
        SubscribableResource,
        "/<string:dataset_id>/<string:version>/subscribable",
        resource_class_args=(keycloak_client, simple_dataset_authorizer_client),
    )
