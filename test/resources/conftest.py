import pytest
from app import app as flask_app
from keycloak import KeycloakOpenID


@pytest.fixture
def mock_client():
    # Configure the application for testing and disable error catching during
    # request handling for better reports. Required in order for exceptions
    # to propagate to the test client.
    # https://flask.palletsprojects.com/en/1.1.x/testing/
    # https://flask.palletsprojects.com/en/1.1.x/api/#flask.Flask.test_client
    flask_app.config["TESTING"] = True

    with flask_app.test_client() as client:
        yield client


valid_token = "valid-token"
username = "janedoe"


@pytest.fixture
def mock_keycloak(monkeypatch):
    def introspect(self, token):
        if token == valid_token:
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
