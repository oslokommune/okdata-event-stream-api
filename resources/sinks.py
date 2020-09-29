import logging
from flask_restful import abort
from flask import request, current_app, g

from resources import Resource
from resources.authorizer import auth
from services import (
    SinkService,
    ResourceConflict,
    ResourceNotFound,
    ResourceUnderDeletion,
    ResourceUnderConstruction,
    SubResourceNotFound,
)

logger = logging.getLogger()


class SinkResource(Resource):
    def __init__(self):
        self.sink_service = SinkService(current_app.dataset_client)

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
    def get(self, dataset_id, version, sink_type):
        try:
            sink = self.sink_service.get_sink(dataset_id, version, sink_type)
            return sink.dict(
                include={"type", "cf_status", "updated_by", "updated_at"},
                by_alias=True,
            )
        except KeyError as e:
            response_msg = f"Invalid sink type: {sink_type}"
            logger.exception(e)
            abort(400, message=response_msg)
        except SubResourceNotFound:
            response_msg = (
                f"Sink of type {sink_type} does not exist on {dataset_id}/{version}"
            )
            abort(404, message=response_msg)
        except Exception as e:
            response_msg = f"Could not get sink of type {sink_type} from event stream {dataset_id}/{version}"
            logger.exception(e)
            abort(500, message=response_msg)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def delete(self, dataset_id, version, sink_type):
        updated_by = g.principal_id
        try:
            self.sink_service.disable_sink(dataset_id, version, sink_type, updated_by)
        except KeyError as e:
            response_msg = f"Invalid sink type: {sink_type}"
            logger.exception(e)
            abort(400, message=response_msg)
        except ResourceNotFound:
            response_msg = f"Event stream with id {dataset_id}/{version} does not exist"
            abort(404, message=response_msg)
        except SubResourceNotFound:
            response_msg = (
                f"Sink of type {sink_type} does not exist on {dataset_id}/{version}"
            )
            abort(404, message=response_msg)
        except ResourceUnderConstruction:
            response_msg = f"Sink of type {sink_type} cannot be disabled since it is being constructed"
            abort(409, response_msg)
        except Exception as e:
            logger.exception(e)
            abort(500, message="Server error")

        return {
            "message": f"Disabled sink of type {sink_type} for stream {dataset_id}/{version}"
        }


class SinksResource(Resource):
    def __init__(self):
        self.sink_service = SinkService(current_app.dataset_client)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def post(self, dataset_id, version):
        try:
            data = request.get_json()
            sink_type = data["type"]
            assert sink_type is not None
        except Exception as e:
            logger.exception(e)
            abort(400, message="No sink type specified in request body")

        try:
            sink = self.sink_service.enable_sink(
                dataset_id, version, sink_type, g.principal_id
            )
            return (
                sink.dict(
                    include={"type", "cf_status", "updated_by", "updated_at"},
                    by_alias=True,
                ),
                201,
            )
        except KeyError as e:
            response_msg = f"Invalid sink type: {sink_type}"
            logger.exception(e)
            abort(400, message=response_msg)
        except ResourceConflict as rc:
            response_msg = str(rc)
            logger.exception(rc)
            abort(409, message=response_msg)
        except ResourceUnderDeletion:
            response_msg = f"Cannot create sink since a sink of type {data['type']} currently is being deleted"
            abort(409, message=response_msg)
        except ResourceNotFound as e:
            response_msg = f"Event stream {dataset_id}/{version} does not exist"
            logger.exception(e)
            abort(404, message=response_msg)
        except Exception as e:
            response_msg = f"Could not update event stream: {str(e)}"
            logger.exception(e)
            abort(500, message=response_msg)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def get(self, dataset_id, version):
        try:
            sinks = self.sink_service.get_sinks(dataset_id, version)
            return [
                sink.dict(
                    include={"type", "cf_status", "updated_by", "updated_at"},
                    by_alias=True,
                )
                for sink in sinks
                if not sink.deleted
            ]
        except Exception as e:
            response_msg = f"Could not get sink list: {str(e)}"
            logger.exception(e)
            abort(500, message=response_msg)
