import pytest
from fastapi.testclient import TestClient
from keycloak import KeycloakOpenID
from okdata.sdk.data.dataset import Dataset
from okdata.resource_auth import ResourceAuthorizer

from app import app


@pytest.fixture
def mock_client(mock_boto):
    return TestClient(app)


valid_token = "valid-token"
valid_token_no_access = "valid-token-no-access"
username = "janedoe"


@pytest.fixture
def mock_authorizer(monkeypatch):
    def has_access(self, bearer_token, scope, resource_name=None, use_whitelist=False):
        return (
            bearer_token == valid_token
            and scope in ["okdata:dataset:update", "okdata:dataset:read"]
            and resource_name.startswith("okdata:dataset:")
        )

    monkeypatch.setattr(ResourceAuthorizer, "has_access", has_access)


@pytest.fixture
def mock_keycloak(monkeypatch):
    def introspect(self, token):
        if token in [valid_token, valid_token_no_access]:
            return {
                "exp": 1594907114,
                "iat": 1594906814,
                "jti": "***REMOVED***",
                "iss": "***REMOVED***",
                "aud": "account",
                "sub": "***REMOVED***",
                "typ": "Bearer",
                "azp": "token-service",
                "session_state": "***REMOVED***",
                "name": "Jane Doe",
                "given_name": "Jane",
                "family_name": "Doe",
                "preferred_username": "janedoe",
                "email": "***REMOVED***",
                "email_verified": False,
                "acr": "1",
                "realm_access": {
                    "roles": ["ok-user", "offline_access", "uma_authorization"]
                },
                "resource_access": {
                    "account": {
                        "roles": [
                            "manage-account",
                            "manage-account-links",
                            "view-profile",
                        ]
                    }
                },
                "scope": "profile email",
                "client_id": "token-service",
                "username": username,
                "active": True,
            }
        else:
            return {"active": False, "username": username}

    monkeypatch.setattr(KeycloakOpenID, "introspect", introspect)


@pytest.fixture()
def mock_dataset_versions(monkeypatch):
    def get_versions(self, dataset_id):
        return [{"id": "my-test-dataset/1", "version": "1"}]

    monkeypatch.setattr(Dataset, "get_versions", get_versions)
