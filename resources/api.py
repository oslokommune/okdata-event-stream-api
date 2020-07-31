import json
import logging
from flask import current_app, make_response
from flask_restful import Api, Resource

from services import ResourceNotFound

logger = logging.getLogger()


class Resource(Resource):
    def get_dataset(self, dataset_id):
        try:
            return current_app.dataset_client.get_dataset(dataset_id)
        except Exception:
            message = f"Could not find dataset: {dataset_id}"
            logger.info(message)
            raise ResourceNotFound(message)


class Api(Api):
    def __init__(self, *args, **kwargs):
        super(Api, self).__init__(*args, **kwargs)
        self.representations = {
            "application/json": output_json,
        }


def output_json(data, code, headers=None):
    if isinstance(data, dict):
        data = json.dumps(data)
    resp = make_response(data, code)
    resp.headers.extend(headers or {})
    return resp
