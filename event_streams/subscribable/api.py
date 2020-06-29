from flask_restful import abort

from event_streams.common import Api, Resource


class SubscribableResource(Resource):
    def get(self, dataset_id, version):
        return {"enabled": True, "cf_status": "active"}

    def put(self, dataset_id, version):
        abort(501)


api = Api(
    "subscribable", __name__, prefix="/<string:dataset_id>/<int:version>/subscribable"
)
api.add_resource(SubscribableResource, "/")
