import re
from functools import wraps
from flask import g, request, make_response
from clients import setup_keycloak_client

# from flask_restful import abort


def requires_auth(method):
    @wraps(method)
    def decorated_function(*args, **kwargs):
        keycloak_client = setup_keycloak_client()
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

        introspected = keycloak_client.introspect(bearer_token)
        if introspected["active"]:
            g.principal_id = introspected["username"]
            g.access_token = bearer_token
            return method(*args, **kwargs)
        else:
            return make_response({"message": "Invalid access token"}, 401)

    return decorated_function


def requires_dataset_ownership(method):
    @wraps(method)
    def decorated_function(*args, **kwargs):
        print(f"principal_id={g.principal_id}")
        # abort(403)
        return method(*args, **kwargs)

    return decorated_function
