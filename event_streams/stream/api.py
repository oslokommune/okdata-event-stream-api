from flask_restful import abort

from event_streams.common import Api, Resource
from event_streams.common.decorators import (
    requires_auth,
    requires_dataset_ownership,
)


class StreamResource(Resource):
    @requires_auth
    @requires_dataset_ownership
    def post(self, dataset_id, version):
        """ Create Kinesis event stream """
        abort(501)

    @requires_auth
    # @requires_dataset_version_exists
    # @requires_dataset_ownership
    def get(self, dataset_id, version):
        # TODO: Use decorators or "mixin" functions?

        # dataset = self.get_dataset_or_404(dataset_id, version)
        # if not dataset.is_owner(g.principal_id):
        #     abort(403, message="Forbidden")
        # event_stream = self.get_event_stream_or_404(dataset_id, version)
        return {
            "id": f"{dataset_id}/{version}",
            "create_raw": True,
            "updated_by": "janedoe",
            "updated_at": "2020-06-23",
            "deleted": False,
            "cf_status": "active",
        }

    def put(self, dataset_id, version):
        abort(501)

    def delete(self, dataset_id, version):
        abort(501)


api = Api("stream", __name__, prefix="/<string:dataset_id>/<int:version>")
api.add_resource(StreamResource, "/")
