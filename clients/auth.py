import re
from flask import g, request, make_response  #
from functools import wraps

from keycloak import KeycloakOpenID

from clients import get_keycloak_config


class KeycloakAuth(object):
    def __init__(self, app=None):
        self.config = get_keycloak_config()

        self.client = KeycloakOpenID(
            server_url=f"{self.config.server_url}/auth/",
            realm_name=self.config.realm_name,
            client_id=self.config.client_id,
            client_secret_key=self.config.client_secret,
        )

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """ Any setup that requires a Flask app, e.g. using app factories """
        pass

    def accept_token(self, view_func):
        @wraps(view_func)
        def decorated_view(*args, **kwargs):
            auth_header = request.headers.get("Authorization")

            if auth_header:
                pattern = re.compile("^(b|B)earer [-0-9a-zA-Z\\._]*$")
                if pattern.match(auth_header):
                    _, bearer_token = auth_header.split()
                else:
                    return make_response(
                        {
                            "message": f"Authorization header must match pattern: '{pattern.pattern}'"
                        },
                        400,
                    )
            else:
                return make_response({"message": "Missing authorization header"}, 400)

            introspected = self.client.introspect(bearer_token)

            if introspected["active"]:
                g.principal_id = introspected["username"]
                g.bearer_token = bearer_token
                return view_func(*args, **kwargs)
            else:
                return make_response({"message": "Invalid access token"}, 401)

        return decorated_view


auth = KeycloakAuth()
