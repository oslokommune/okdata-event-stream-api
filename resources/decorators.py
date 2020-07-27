import re
from functools import wraps
from flask import g, request, make_response, current_app


def requires_auth(method):
    @wraps(method)
    def decorated_function(self, **kwargs):
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

        introspected = current_app.keycloak_client.introspect(bearer_token)
        if introspected["active"]:
            g.principal_id = introspected["username"]
            g.bearer_token = bearer_token
            return method(self, **kwargs)
        else:
            return make_response({"message": "Invalid access token"}, 401)

    return decorated_function


def requires_dataset_ownership(method):
    @wraps(method)
    def decorated_function(self, dataset_id, **kwargs):
        dataset_access = current_app.simple_dataset_authorizer_client.check_dataset_access(
            dataset_id, bearer_token=g.bearer_token
        )

        if dataset_access["access"]:
            return method(self, dataset_id, **kwargs)
        else:
            return make_response({"message": "Forbidden"}, 403)

    return decorated_function
