import logging
from datetime import date

from fastapi import APIRouter, Depends, Path, Query
from pydantic import BaseModel

from resources.authorizer import authorize, version_exists
from resources.origo_clients import dataset_client
from resources.errors import ErrorResponse, error_message_models
from services import ElasticsearchDataService

logger = logging.getLogger()
router = APIRouter()


def query_service(dataset_client=Depends(dataset_client)) -> ElasticsearchDataService:
    return ElasticsearchDataService(dataset_client)


class StandardResponse(BaseModel):
    message: str


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


@router.post(
    "/{dataset_id}/{version}",
    dependencies=[
        Depends(authorize("okdata:dataset:write")),
        Depends(version_exists),
    ],
    response_model=StandardResponse,
)
def post_events(dataset_id: str, version: str):
    ""
