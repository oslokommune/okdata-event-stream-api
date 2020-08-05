import logging
import json
from flask_restful import Resource, abort, reqparse
from datetime import datetime

from resources.authorizer import auth
from services import ElasticsearchDataService

logger = logging.getLogger()


# Returns the countnumber based on provided dates
class StreamStatisticsResource(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument("from_date", type=str)
        self.parser.add_argument("to_date", type=str)

    @auth.accepts_token
    @auth.requires_dataset_ownership
    @auth.requires_dataset_version_exists
    def get(self, dataset_id, version):
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
            f"Getting count event with id: {dataset_id}-{version} from {from_date} to {to_date}"
        )
        event_history = ElasticsearchDataService()
        data = event_history.get_event_count(dataset_id, version, from_date, to_date)
        if not data:
            abort(400, message=f"Could not find event: {dataset_id}/{version}")
        return json.dumps(data)
