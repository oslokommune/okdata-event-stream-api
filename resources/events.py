import logging

from fastapi import APIRouter, Depends, Path, Query
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
def get(
    *,
    dataset_id: str = Path(..., min_length=3, max_length=70, regex="^[a-z0-9-]*$"),
    version: str = Path(..., min_length=1),
    from_date: date,
    to_date: date,
    page: int = Query(1, gt=0),
    page_size: int = Query(10, gt=0, lt=10001),
    query_service=Depends(query_service),
):
    logger.info(
        f"Getting history about event with id: {dataset_id}-{version} from {from_date} to {to_date}"
    )
    data = query_service.get_event_by_date(
        dataset_id, version, from_date, to_date, page, page_size
    )

    if not data:
        raise ErrorResponse(400, f"Could not find event: {dataset_id}/{version}")

    return data


@router.get(
    "/aggregation",
    dependencies=[Depends(authorize("okdata:dataset:read")), Depends(version_exists)],
    responses=error_message_models(400),
)
def get_single_aggr(
    *,
    dataset_id: str = Path(..., min_length=3, max_length=70, regex="^[a-z0-9-]*$"),
    version: str = Path(..., min_length=1),
    from_date: date,
    to_date: date,
    field: str,
    size: int,
    query_service=Depends(query_service),
):
    logger.info(
        f"Getting aggregated data with id: {dataset_id}-{version} from {from_date} to {to_date} by term {field}"
    )
    data = query_service.get_single_aggregation(
        dataset_id=dataset_id,
        version=version,
        from_date=from_date,
        to_date=to_date,
        field=field,
        size=size,
    )

    if not data:
        raise ErrorResponse(400, f"Could not find event: {dataset_id}/{version}")

    return data


@router.get(
    "/aggregation/eventsByRange",
    dependencies=[Depends(authorize("okdata:dataset:read")), Depends(version_exists)],
    responses=error_message_models(400),
)
def get_events_by_range(
    *,
    dataset_id: str = Path(..., min_length=3, max_length=70, regex="^[a-z0-9-]*$"),
    version: str = Path(..., min_length=1),
    from_date: date,
    to_date: date,
    field: str,
    from_range: str,
    to_range: str,
    query_service=Depends(query_service),
):
    logger.info(
        f"Getting aggregated data with id: {dataset_id}-{version} from {from_date} to {to_date} by term {field}"
    )
    data = query_service.count_events_by_range(
        dataset_id=dataset_id,
        version=version,
        from_date=from_date,
        to_date=to_date,
        from_range=from_range,
        to_range=to_range,
    )

    if not data:
        raise ErrorResponse(400, f"Could not find event: {dataset_id}/{version}")

    return data


@router.get(
    "/aggregation/histogram",
    dependencies=[Depends(authorize("okdata:dataset:read")), Depends(version_exists)],
    responses=error_message_models(400),
)
def get_histogram(
    *,
    dataset_id: str = Path(..., min_length=3, max_length=70, regex="^[a-z0-9-]*$"),
    version: str = Path(..., min_length=1),
    from_date: str,
    to_date: str,
    interval: str,
    query_service=Depends(query_service),
):
    logger.info(
        f"Getting aggregated data in histogram format with id: {dataset_id}-{version} - from {from_date} to {to_date}"
    )
    data = query_service.get_histogram(
        dataset_id=dataset_id,
        version=version,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
    )

    if not data:
        raise ErrorResponse(400, f"Could not find event: {dataset_id}/{version}")

    return data
