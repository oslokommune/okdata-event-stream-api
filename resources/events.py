import logging
from datetime import date
from typing import List

from botocore.client import ClientError
from fastapi import APIRouter, Depends, Path, Query, status
from okdata.aws.logging import log_add
from pydantic import BaseModel
from requests.exceptions import HTTPError

from resources.authorizer import authorize, version_exists
from resources.errors import ErrorResponse, error_message_models
from resources.origo_clients import dataset_client
from services import ElasticsearchDataService, EventService, PutRecordsError

logger = logging.getLogger()
router = APIRouter()


def query_service(dataset_client=Depends(dataset_client)) -> ElasticsearchDataService:
    return ElasticsearchDataService(dataset_client)


def event_service(dataset_client=Depends(dataset_client)) -> EventService:
    return EventService(dataset_client)


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


class Events(BaseModel):
    events: List[dict]


@router.post(
    "",
    dependencies=[
        Depends(authorize("okdata:dataset:write")),
        Depends(version_exists),
    ],
    responses=error_message_models(
        status.HTTP_400_BAD_REQUEST,
        status.HTTP_403_FORBIDDEN,
        status.HTTP_404_NOT_FOUND,
        status.HTTP_422_UNPROCESSABLE_ENTITY,
        status.HTTP_500_INTERNAL_SERVER_ERROR,
    ),
)
def post(
    *,
    dataset_id: str = Path(..., min_length=3, max_length=70, regex="^[a-z0-9-]*$"),
    version: str = Path(..., min_length=1),
    event_service=Depends(event_service),
    events: Events,
):
    log_add(dataset_id=dataset_id, version=version)

    try:
        dataset = event_service.dataset_client.get_dataset(dataset_id)
    except HTTPError:
        raise ErrorResponse(
            status.HTTP_404_NOT_FOUND, f"Dataset '{dataset_id}' not found"
        )

    try:
        return event_service.send_events(dataset, version, events.events)
    except PutRecordsError as e:
        log_add(failed_records=e.num_records)
        raise ErrorResponse(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e))
    except ClientError:
        raise ErrorResponse(status.HTTP_500_INTERNAL_SERVER_ERROR, "Server error")
