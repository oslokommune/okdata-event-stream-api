import logging
from fastapi import Depends, APIRouter
from datetime import date

from resources.authorizer import dataset_owner, version_exists
from resources.origo_clients import dataset_client
from resources.errors import ErrorResponse, error_message_models
from services import ElasticsearchDataService

logger = logging.getLogger()
router = APIRouter()


def query_service(dataset_client=Depends(dataset_client)) -> ElasticsearchDataService:
    return ElasticsearchDataService(dataset_client)


@router.get(
    "",
    dependencies=[Depends(dataset_owner), Depends(version_exists)],
    responses=error_message_models(400),
)
def count(
    dataset_id: str,
    version: str,
    from_date: date,
    to_date: date,
    query_service=Depends(query_service),
):
    logger.info(
        f"Getting count event with id: {dataset_id}-{version} from {from_date} to {to_date}"
    )

    data = query_service.get_event_count(dataset_id, version, from_date, to_date)

    if not data:
        raise ErrorResponse(400, f"Could not find event: {dataset_id}/{version}")

    return data
