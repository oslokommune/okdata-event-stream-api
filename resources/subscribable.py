import logging
from flask import request, g, current_app, make_response
from flask_restful import abort


from resources import Resource
from resources.authorizer import auth
from services import SubscribableService, ResourceNotFound, ResourceConflict


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SubscribableResource(Resource):
    def __init__(self):
        self.subscribable_service = SubscribableService(current_app.dataset_client)

    @auth.accepts_token
    def get(self, dataset_id, version):
        try:
            subscribable = self.subscribable_service.get_subscribable(
                dataset_id, version
            )
        except ResourceNotFound:
            abort(
                404,
                message=f"Event stream with id {dataset_id}/{version} does not exist",
            )
        except Exception as e:
            logger.exception(e)
            abort(500, message="Server error")

        return make_response(subscribable.json(exclude={"cf_stack_template"}), 200)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    def put(self, dataset_id, version):
        try:
            request_body = request.get_json()
            enabled = request_body["enabled"]
            assert isinstance(enabled, bool)
            updated_by = g.principal_id
        except Exception as e:
            logger.exception(e)
            abort(400, message="Bad request")

        service_call = (
            self.subscribable_service.enable_subscribable
            if enabled
            else self.subscribable_service.disable_subscribable
        )

        try:
            subscribable = service_call(dataset_id, version, updated_by)
        except ResourceNotFound:
            abort(
                404,
                message=f"Event stream with id {dataset_id}/{version} does not exist",
            )
        except ResourceConflict:
            subscribable_state = (
                "already subscribable" if enabled else "not currently subscribable"
            )
            abort(
                409,
                message=f"Event stream with id {dataset_id}/{version} is {subscribable_state}",
            )
        except Exception as e:
            logger.exception(e)
            abort(500, message="Server error")

        return make_response(subscribable.json(exclude={"cf_stack_template"}), 200)
