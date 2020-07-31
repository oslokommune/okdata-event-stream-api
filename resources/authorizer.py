import re
from flask import g, request, make_response, current_app
from functools import wraps

from origo.dataset_authorizer.simple_dataset_authorizer_client import (
    SimpleDatasetAuthorizerClient,
)

from clients import setup_keycloak_client, setup_origo_sdk, get_keycloak_config


class Authorizer(object):
    def __init__(self, app=None):
        self.config = get_keycloak_config()

        self.keycloak_client = setup_keycloak_client(self.config)

        self.simple_dataset_authorizer_client = setup_origo_sdk(
            self.config, SimpleDatasetAuthorizerClient
        )

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """ Any setup that requires a Flask app, e.g. using app factories """
        pass

    def accepts_token(self, view_func):
        @wraps(view_func)
        def decorated(resource, **kwargs):
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

            introspected = self.keycloak_client.introspect(bearer_token)

            if introspected["active"]:
                g.principal_id = introspected["username"]
                g.bearer_token = bearer_token
                return view_func(resource, **kwargs)
            else:
                return make_response({"message": "Invalid access token"}, 401)

        return decorated

    def requires_dataset_ownership(self, view_func):
        @wraps(view_func)
        def decorated(resource, **kwargs):
            dataset_access = self.simple_dataset_authorizer_client.check_dataset_access(
                kwargs["dataset_id"], bearer_token=g.bearer_token
            )

            if dataset_access["access"]:
                return view_func(resource, **kwargs)
            else:
                return make_response({"message": "Forbidden"}, 403)

        return decorated

    def requires_dataset_version_exists(self, view_func):
        @wraps(view_func)
        def decorated(resource, **kwargs):
            versions = current_app.dataset_client.get_versions(kwargs["dataset_id"])
            for version in versions:
                if version["version"] == kwargs["version"]:
                    return view_func(resource, **kwargs)
            return make_response(
                {
                    "message": f"Version: {kwargs['version']} for dataset '{kwargs['dataset_id']}' not found"
                },
                404,
            )

        return decorated


auth = Authorizer()
