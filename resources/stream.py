import logging
from flask_restful import abort
from flask import request, make_response, g, jsonify, current_app

from resources import Resource
from resources.authorizer import auth
from services import EventStreamService, ResourceConflict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class StreamResource(Resource):
    def __init__(self):
        self.event_stream_service = EventStreamService(current_app.dataset_client)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    def post(self, dataset_id, version):
        """ Create Kinesis event stream """
        request_body = request.get_json()
        create_raw = request_body.get("create_raw", True) if request_body else True
        updated_by = g.principal_id
        try:
            event_stream = self.event_stream_service.create_event_stream(
                dataset_id=dataset_id,
                version=version,
                updated_by=updated_by,
                create_raw=create_raw,
            )
        except ResourceConflict:
            response_msg = f"Event stream with id {dataset_id}/{version} already exist"
            return make_response(jsonify({"message": response_msg}), 409)
        except Exception as e:
            logger.exception(e)
            return make_response(jsonify({"message": "Server error"}), 500)

        return make_response(event_stream.json(), 201)

    @auth.accepts_token
    # @auth.requires_dataset_ownership
    # @requires_dataset_version_exists
    def get(self, dataset_id, version):
        # TODO: Use decorators or "mixin" functions?

        # dataset = self.get_dataset_or_404(dataset_id, version)
        # if not dataset.is_owner(g.principal_id):
        #     abort(403, message="Forbidden")
        # event_stream = self.get_event_stream_or_404(dataset_id, version)
        return make_response(
            {
                "id": f"{dataset_id}/{version}",
                "create_raw": True,
                "updated_by": g.principal_id,
                "updated_at": "2020-06-23",
                "deleted": False,
                "cf_status": "active",
            },
            200,
        )

    def put(self, dataset_id, version):
        abort(501)

    def delete(self, dataset_id, version):
        abort(501)
