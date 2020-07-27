from flask_restful import abort

from resources import Resource


class SinkResource(Resource):
    def __init__(self, keycloak_client, simple_dataset_authorizer_client):
        self.keycloak_client = keycloak_client
        self.simple_dataset_authorizer_client = simple_dataset_authorizer_client

    def put(self, dataset_id, version):
        abort(501)

    def get(self, dataset_id, version):
        return {"enabled": True, "cf_status": "active"}

    def delete(self, dataset_id, version):
        abort(501)


class SinksResource(Resource):
    def __init__(self, keycloak_client, simple_dataset_authorizer_client):
        self.keycloak_client = keycloak_client
        self.simple_dataset_authorizer_client = simple_dataset_authorizer_client

    def get(self, dataset_id, version):
        return [{"enabled": True, "cf_status": "active"}]
