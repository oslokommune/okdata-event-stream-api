import logging
from flask import make_response
from flask_restful import abort


from resources import Resource
from resources.authorizer import auth
from services import SubscribableService, ResourceNotFound


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SubscribableResource(Resource):
    def __init__(self):
        self.subscribable_service = SubscribableService()

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

    def put(self, dataset_id, version):
        abort(501)
