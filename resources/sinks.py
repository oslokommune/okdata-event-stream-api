import logging
from flask_restful import abort
from flask import request, current_app, g, jsonify

from database import Sink, EventStream
from database.models import SinkType
from resources import Resource
from resources.authorizer import auth
from services import (
    EventStreamSinkService,
    ResourceConflict,
    ResourceNotFound,
    SubResourceNotFound,
)

logger = logging.getLogger()


class SinkResource(Resource):
    def __init__(self):
        self.sink_service = EventStreamSinkService(current_app.dataset_client)

    def post(self, dataset_id, version):
        abort(501)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def put(self, dataset_id, version, sink_id):
        abort(501)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def get(self, dataset_id, version, sink_id):
        try:
            return self.sink_service.get_sink_for_api(dataset_id, version, sink_id)
        except SubResourceNotFound:
            response_msg = f"sink {sink_id} does not exist on {dataset_id}/{version}"
            abort(404, message=response_msg)
        except Exception as e:
            response_msg = (
                f"Could not get sink {sink_id} from event stream {dataset_id}/{version}"
            )
            logger.exception(e)
            abort(500, message=response_msg)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def delete(self, dataset_id, version, sink_id):
        updated_by = g.principal_id
        try:
            self.sink_service.delete_sink(dataset_id, version, sink_id, updated_by)
        except ResourceNotFound:
            response_msg = f"Event stream with id {dataset_id}/{version} does not exist"
            abort(404, message=response_msg)
        except SubResourceNotFound:
            response_msg = (
                f"Sink with id {sink_id} does not exist on {dataset_id}/{version}"
            )
            abort(404, message=response_msg)
        except Exception as e:
            logger.exception(e)
            abort(500, message="Server error")

        return {"message": f"Deleted sink {sink_id} from stream {dataset_id}/{version}"}


class SinksResource(Resource):
    def __init__(self):
        self.sink_service = EventStreamSinkService(current_app.dataset_client)

    def sink_exists(self, event_stream: EventStream, sink_type: SinkType) -> bool:
        existing_sinks = event_stream.sinks
        for existing_sink in existing_sinks:
            if existing_sink.type == sink_type.value:
                return True
        return False

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def post(self, dataset_id, version):
        event_stream = self.sink_service.get_event_stream(dataset_id, version)
        if event_stream is None:
            response_msg = f"Event stream: {dataset_id}/{version} does not exist"
            abort(404, message=response_msg)
        try:
            data = request.get_json()
            sink_type = SinkType[data["type"].upper()]
            if self.sink_exists(event_stream, sink_type):
                raise ResourceConflict(
                    f"Sink: {sink_type.value} already exists on {event_stream.id}"
                )
        except KeyError as e:
            response_msg = f"Invalid sink type: {data['type']}"
            logger.exception(e)
            abort(400, message=response_msg)
        except ResourceConflict as rc:
            response_msg = str(rc)
            logger.exception(rc)
            abort(409, message=response_msg)
        except Exception as e:
            logger.exception(e)
            response_msg = "Could not decode data for sink: ensure a valid json object is available"
            abort(400, message=response_msg)

        sink = Sink(type=sink_type.value)
        try:
            self.sink_service.add_sink(
                event_stream, dataset_id, version, sink, g.principal_id
            )
            return {"type": sink_type.value, "id": sink.id}, 201
        except Exception as e:
            response_msg = f"Could not update event stream: {str(e)}"
            logger.exception(e)
            abort(500, message=response_msg)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def get(self, dataset_id, version):
        try:
            sinks = self.sink_service.get_sinks_for_api(dataset_id, version)
            return jsonify(sinks)
        except Exception as e:
            response_msg = f"Could not get sink list: {str(e)}"
            logger.exception(e)
            abort(500, message=response_msg)
