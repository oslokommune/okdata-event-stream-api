import logging
from flask import current_app
from flask_restful import Resource

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
