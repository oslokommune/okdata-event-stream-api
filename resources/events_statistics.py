import logging
from fastapi import Depends, APIRouter
from datetime import date

from resources.authorizer import authorize, version_exists
from resources.origo_clients import dataset_client
from resources.errors import ErrorResponse, error_message_models
from services import ElasticsearchDataService

logger = logging.getLogger()
router = APIRouter()


def query_service(dataset_client=Depends(dataset_client)) -> ElasticsearchDataService:
    return ElasticsearchDataService(dataset_client)


@router.get(
    "",
    dependencies=[Depends(authorize("okdata:dataset:read")), Depends(version_exists)],
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

@router.get(
    "/granular",
    dependencies=[Depends(authorize("okdata:dataset:read")), Depends(version_exists)],
    responses=error_message_models(400),
)
def count(
    dataset_id: str,
    version: str,
    from_range: str,
    to_range: str,
    pattern: str,
    query_service=Depends(query_service),
):
    logger.info(
        f"Getting granular count event with id: {dataset_id}-{version} from {from_range} to {to_range} in pattern {pattern}"
    )
    data = query_service.get_event_count_granular(dataset_id=dataset_id, version=version, from_range=from_range, to_range=to_range, pattern=pattern)

    if data is None:
        raise ErrorResponse(400, f"Could not find event: {dataset_id}/{version}")

    return data

@router.get(
    "/average",
    dependencies=[Depends(authorize("okdata:dataset:read")), Depends(version_exists)],
    responses=error_message_models(400),
)
def count(
    dataset_id: str,
    version: str,
    interval: str,
    field: str,
    query_service=Depends(query_service),
):
    logger.info(
        f"Getting average count event with id: {dataset_id}-{version} on field {field} with interval {interval}"
    )
    data = query_service.get_average_count(dataset_id, version, field, interval)

    if data is None:
        raise ErrorResponse(400, f"Could not find event: {dataset_id}/{version}")

    return data