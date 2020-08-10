import logging
from flask_restful import abort
from flask import request, g, current_app

from resources import Resource
from resources.authorizer import auth
from services import EventStreamService, ResourceConflict, ResourceNotFound

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class StreamResource(Resource):
    def __init__(self):
        self.event_stream_service = EventStreamService(current_app.dataset_client)

    def get_event_stream(self, dataset_id, version):
        event_stream = self.event_stream_service.get_event_stream(dataset_id, version)
        if event_stream is None:
            message = f"Could not find stream: {dataset_id}/{version}"
            logger.info(message)
            raise ResourceNotFound(message)
        return event_stream

    @auth.accepts_token
    @auth.requires_dataset_ownership
    def post(self, dataset_id, version):
        """
        Create Kinesis event stream:
            curl -H "Authorization: bearer $TOKEN" -H "Content-Type: application/json" --data '{"type":"s3"}' -XPOST http://127.0.0.1:8080/{dataset-id}/{version}
        """
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
            abort(409, message=response_msg)
        except Exception as e:
            logger.exception(e)
            abort(500, message="Server error")

        return event_stream.json(), 201

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def get(self, dataset_id, version):
        try:
            dataset = self.get_dataset(dataset_id)
            event_stream = self.get_event_stream(dataset_id, version)
        except ResourceNotFound as e:
            abort(404, message=str(e))

        return event_stream.copy(
            include={
                "id",
                "create_raw",
                "updated_by",
                "updated_at",
                "deleted",
                "cf_status",
            },
            update={"confidentiality": dataset["confidentiality"]},
        ).json()

    def put(self, dataset_id, version):
        abort(501)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    def delete(self, dataset_id, version):
        updated_by = g.principal_id
        try:
            self.event_stream_service.delete_event_stream(
                dataset_id=dataset_id, version=version, updated_by=updated_by
            )
        except ResourceNotFound:
            response_msg = f"Event stream with id {dataset_id}/{version} does not exist"
            abort(404, message=response_msg)

        # In the future this endpoint will also delete sub-resources. Until then return 409 if subresources exist. Oyvind Nygard 2020-09-30
        except ResourceConflict:
            response_msg = f"Event stream with id {dataset_id}/{version} contains sub-resources. Delete all related event-sinks and disable event subscription"
            abort(409, message=response_msg)
        except Exception as e:
            logger.exception(e)
            abort(500, message="Server error")

        return {"message": f"Deleted event stream with id {dataset_id}/{version}"}
