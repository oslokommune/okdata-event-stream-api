import pytest
from keycloak import KeycloakOpenID
from origo.dataset_authorizer.simple_dataset_authorizer_client import (
    SimpleDatasetAuthorizerClient,
)


@pytest.fixture
def mock_client(mock_boto):
    from app import app as flask_app

    # Configure the application for testing and disable error catching during
    # request handling for better reports. Required in order for exceptions
    # to propagate to the test client.
    # https://flask.palletsprojects.com/en/1.1.x/testing/
    # https://flask.palletsprojects.com/en/1.1.x/api/#flask.Flask.test_client
    flask_app.config["TESTING"] = True

    with flask_app.test_client() as client:
        yield client


valid_token = "valid-token"
valid_token_no_access = "valid-token-no-access"
username = "janedoe"


@pytest.fixture
def mock_authorizer(monkeypatch):
    def dataset_access(self, dataset, bearer_token):
        if bearer_token == valid_token:
            return {"access": True}
        else:
            return {"access": False}

    monkeypatch.setattr(
        SimpleDatasetAuthorizerClient, "check_dataset_access", dataset_access
    )


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
