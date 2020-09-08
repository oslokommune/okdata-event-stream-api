import json
import logging
import datetime
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


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)
        elif isinstance(obj, datetime.date):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def output_json(data, code, headers=None):
    data = json.dumps(data, cls=JSONEncoder)
    resp = make_response(data, code)
    resp.headers.extend(headers or {})
    return resp
