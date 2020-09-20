from fastapi import Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from clients import setup_keycloak_client, get_keycloak_config

from .errors import ErrorResponse
from .origo_clients import simple_dataset_authorizer_client, dataset_client


def keycloak_client(keycloak_config=Depends(get_keycloak_config)):
    return setup_keycloak_client(keycloak_config)


bearer_token = HTTPBearer(scheme_name="Keycloak token")


class AuthInfo:
    principal_id: str
    bearer_token: str

    def __init__(
        self,
        authorization: HTTPAuthorizationCredentials = Depends(bearer_token),
        keycloak_client=Depends(keycloak_client),
    ):
        introspected = keycloak_client.introspect(authorization.credentials)

        if not introspected["active"]:
            raise ErrorResponse(401, "Invalid access token")

        self.principal_id = introspected["username"]
        self.bearer_token = authorization.credentials


def dataset_owner(
    dataset_id: str,
    auth_info: AuthInfo = Depends(),
    simple_dataset_authorizer_client=Depends(simple_dataset_authorizer_client),
):
    dataset_access = simple_dataset_authorizer_client.check_dataset_access(
        dataset_id, bearer_token=auth_info.bearer_token
    )
    if not dataset_access["access"]:
        raise ErrorResponse(403, "Forbidden")


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
