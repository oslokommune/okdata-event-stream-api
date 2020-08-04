import logging
from flask_restful import Resource, abort, reqparse
from datetime import datetime

from services import ElasticsearchDataService

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Gets resources from Flask-endpoint, returns events based on provided dates
class StreamEventResource(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()

    def get(self, dataset_id, version):
        self.parser.add_argument("from_date", type=str)
        self.parser.add_argument("to_date", type=str)
        args = self.parser.parse_args()
        try:
            from_date = datetime.fromisoformat(args["from_date"])
            to_date = datetime.fromisoformat(args["to_date"])
        except ValueError as data_error:
            logger.error("Error while processing date data. Try isoformat.")
            logger.exception(data_error)
            abort(400, message="Error while processing date data. Try isoformat")
        except TypeError as no_date_error:
            logger.error("No date provided")
            logger.exception(no_date_error)
            abort(400, message="No date provided")

        logger.info(
            f"Getting history about event with id: {dataset_id}-{version} from {from_date} to {to_date}"
        )
        event_history = ElasticsearchDataService()
        data = event_history.get_event_by_date(dataset_id, version, from_date, to_date)
        if not data:
            abort(400, message="No event found for provided id")
        logger.warning("Returning data")
        return data
