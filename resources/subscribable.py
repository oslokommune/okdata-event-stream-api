from flask_restful import abort

from resources import Resource
from resources.authorizer import auth


class SubscribableResource(Resource):
    @auth.accepts_token
    @auth.requires_dataset_ownership
    def get(self, dataset_id, version):
        return {"enabled": True, "cf_status": "active"}

    def put(self, dataset_id, version):
        abort(501)
