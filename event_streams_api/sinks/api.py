from flask_restful import abort

from event_streams_api.common import Api, Resource


class SinksResource(Resource):
    def post(self, dataset_id, version):
        return {"enabled": True, "cf_status": "active"}

    def put(self, dataset_id, version):
        abort(501)


class SinkResource(Resource):
    def put(self, dataset_id, version):
        abort(501)

    def get(self, dataset_id, version):
        return {"enabled": True, "cf_status": "active"}

    def delete(self, dataset_id, version):
        abort(501)


api = Api("sinks", __name__, prefix="/<string:dataset_id>/<int:version>/sinks")
api.add_resource(SinksResource, "/")
api.add_resource(SinkResource, "/<string:sink_id>")
