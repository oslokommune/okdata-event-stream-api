from flask_restful import abort

from resources import Resource


class SinkResource(Resource):
    def put(self, dataset_id, version):
        abort(501)

    def get(self, dataset_id, version):
        return {"enabled": True, "cf_status": "active"}

    def delete(self, dataset_id, version):
        abort(501)


class SinksResource(Resource):
    def get(self, dataset_id, version):
        return [{"enabled": True, "cf_status": "active"}]
