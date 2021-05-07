from fastapi import Depends
from okdata.sdk.data.dataset import Dataset


from clients import setup_origo_sdk, get_keycloak_config


class OrigoSDK:
    def __init__(self, sdk):
        self.sdk = sdk

    def __call__(self, keycloak_config=Depends(get_keycloak_config)):
        return setup_origo_sdk(keycloak_config, self.sdk)


dataset_client = OrigoSDK(Dataset)
