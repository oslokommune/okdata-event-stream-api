from fastapi import Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from okdata.resource_auth import ResourceAuthorizer

from clients import setup_keycloak_client, get_keycloak_config
from .errors import ErrorResponse
from .origo_clients import dataset_client


def keycloak_client(keycloak_config=Depends(get_keycloak_config)):
    return setup_keycloak_client(keycloak_config)


def resource_authorizer() -> ResourceAuthorizer:
    return ResourceAuthorizer()


http_bearer = HTTPBearer(scheme_name="Keycloak token")


class AuthInfo:
    principal_id: str
    bearer_token: str

    def __init__(
        self,
        authorization: HTTPAuthorizationCredentials = Depends(http_bearer),
        keycloak_client=Depends(keycloak_client),
    ):
        introspected = keycloak_client.introspect(authorization.credentials)

        if not introspected["active"]:
            raise ErrorResponse(401, "Invalid access token")

        self.principal_id = introspected["username"]
        self.bearer_token = authorization.credentials


def authorize(scope: str):
    def _verify_permission(
        dataset_id: str,
        auth_info: AuthInfo = Depends(),
        resource_authorizer: ResourceAuthorizer = Depends(resource_authorizer),
    ):
        if not resource_authorizer.has_access(
            auth_info.bearer_token, scope, f"okdata:dataset:{dataset_id}"
        ):
            raise ErrorResponse(403, "Forbidden")

    return _verify_permission


def dataset_exists(dataset_id: str, dataset_client=Depends(dataset_client)) -> dict:
    try:
        dataset = dataset_client.get_dataset(dataset_id)
        return dataset
    except Exception:
        message = f"Could not find dataset: {dataset_id}"
        raise ErrorResponse(404, message)


def version_exists(
    dataset_id: str, version: str, dataset_client=Depends(dataset_client)
) -> dict:
    try:
        versions = dataset_client.get_versions(dataset_id)
        for v in versions:
            if v["version"] == version:
                return v
    except Exception:
        pass

    raise ErrorResponse(
        404,
        f"Version: {version} for dataset '{dataset_id}' not found",
    )


def is_event_source(dataset_id: str, dataset=Depends(dataset_exists)) -> bool:
    source_type = dataset.get("source", {}).get("type")
    if source_type == "event":
        return True

    message = f"Invalid source type '{source_type}' for dataset {dataset_id}. Dataset source must be of type: 'event'"
    raise ErrorResponse(400, message)
