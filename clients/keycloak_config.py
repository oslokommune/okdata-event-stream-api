import os
from dataclasses import dataclass
import boto3


@dataclass
class KeycloakConfig:
    client_id: str
    client_secret: str
    server_url: str
    realm_name: str


def get_keycloak_config():
    ssm_client = SSMClient()

    client_id = "event-stream-api"

    client_secret = ssm_client.get_ssm_parameter(
        f"/dataplatform/{client_id}/keycloak-client-secret", with_decryption=True,
    )

    server_url = ssm_client.get_ssm_parameter(
        "/dataplatform/shared/keycloak-server-url"
    )

    realm_name = ssm_client.get_ssm_parameter("/dataplatform/shared/keycloak-realm")

    return KeycloakConfig(
        client_id=client_id,
        client_secret=client_secret,
        server_url=server_url,
        realm_name=realm_name,
    )


class SSMClient:
    def __init__(self):
        self.client = boto3.client("ssm", region_name=os.environ["AWS_REGION"])

    def get_ssm_parameter(self, parameter_name, with_decryption=False):
        parameter = self.client.get_parameter(
            Name=parameter_name, WithDecryption=with_decryption
        )
        return parameter["Parameter"]["Value"]
