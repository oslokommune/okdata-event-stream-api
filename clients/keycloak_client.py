import os
import boto3
from keycloak import KeycloakOpenID


def setup_keycloak_client():
    ssm_client = boto3.client("ssm", region_name=os.environ["AWS_REGION"])
    server_url = ssm_client.get_parameter(
        Name="/dataplatform/shared/keycloak-server-url"
    )["Parameter"]["Value"]
    realm_name = ssm_client.get_parameter(Name="/dataplatform/shared/keycloak-realm")[
        "Parameter"
    ]["Value"]
    keycloak_client_id = "event-stream-api"
    client_secret = ssm_client.get_parameter(
        Name=f"/dataplatform/{keycloak_client_id}/keycloak-client-secret",
        WithDecryption=True,
    )["Parameter"]["Value"]

    return KeycloakOpenID(
        server_url=f"{server_url}/auth/",
        client_id=keycloak_client_id,
        realm_name=realm_name,
        client_secret_key=client_secret,
    )
