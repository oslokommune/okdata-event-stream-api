from okdata.sdk import SDK
from okdata.sdk.config import Config

from .keycloak_config import KeycloakConfig


def setup_origo_sdk(keycloak_config: KeycloakConfig, sdk: SDK):
    origo_config = Config()
    origo_config.config["client_id"] = keycloak_config.client_id
    origo_config.config["client_secret"] = keycloak_config.client_secret
    origo_config.config["cacheCredentials"] = False

    sdk_instance = sdk(config=origo_config)
    return sdk_instance
