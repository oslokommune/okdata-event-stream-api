from flask_restful import abort

from clients.auth import auth
from resources import Resource


class SubscribableResource(Resource):
    @auth.accept_token
    def get(self, dataset_id, version):
        return {"enabled": True, "cf_status": "active"}

    def put(self, dataset_id, version):
        abort(501)
