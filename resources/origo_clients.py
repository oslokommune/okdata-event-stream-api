from fastapi import Depends
from origo.data.dataset import Dataset
from origo.dataset_authorizer.simple_dataset_authorizer_client import (
    SimpleDatasetAuthorizerClient,
)
from clients import setup_origo_sdk, get_keycloak_config


class OrigoSDK:
    def __init__(self, sdk):
        self.sdk = sdk

    def __call__(self, keycloak_config=Depends(get_keycloak_config)):
        return setup_origo_sdk(keycloak_config, self.sdk)


simple_dataset_authorizer_client = OrigoSDK(SimpleDatasetAuthorizerClient)
dataset_client = OrigoSDK(Dataset)
