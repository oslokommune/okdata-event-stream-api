import logging
from typing import Optional
from datetime import datetime
from fastapi import Depends, APIRouter, status, Body
from pydantic import BaseModel, Field

from resources.authorizer import (
    AuthInfo,
    dataset_owner,
    dataset_exists,
    version_exists,
    is_event_source,
)
from resources.origo_clients import dataset_client
from resources.errors import ErrorResponse, error_message_models, Message
from services import (
    EventStreamService,
    ResourceConflict,
    ResourceNotFound,
)
from util import CONFIDENTIALITY_MAP

logger = logging.getLogger()
logger.setLevel(logging.INFO)

router = APIRouter()


def event_stream_service(dataset_client=Depends(dataset_client)) -> EventStreamService:
    return EventStreamService(dataset_client)


def get_event_stream(
    dataset_id: str, version: str, event_stream_service=Depends(event_stream_service)
):
    event_stream = event_stream_service.get_event_stream(dataset_id, version)
    if event_stream is None:
        message = f"Could not find stream: {dataset_id}/{version}"
        logger.info(message)
        raise ErrorResponse(status.HTTP_404_NOT_FOUND, message)
    return event_stream


class EventStreamIn(BaseModel):
    create_raw: Optional[bool] = True


class EventStreamOut(BaseModel):
    id: str
    create_raw: bool
    updated_by: Optional[str]
    updated_at: datetime
    deleted: bool
    cf_status: str = Field("INACTIVE", max_length=20, alias="status")


class EventStreamWithAcccessRightsOut(EventStreamOut):
    accessRights: str
    confidentiality: str  # Temporary: Remove once phased out in the CLI/SDK


@router.post(
    "",
    dependencies=[
        Depends(dataset_owner),
        Depends(is_event_source),
        Depends(version_exists),
    ],
    response_model=EventStreamOut,
    status_code=status.HTTP_201_CREATED,
    responses=error_message_models(
        status.HTTP_400_BAD_REQUEST,
        status.HTTP_404_NOT_FOUND,
        status.HTTP_409_CONFLICT,
        status.HTTP_500_INTERNAL_SERVER_ERROR,
    ),
)
def post(
    dataset_id: str,
    version: str,
    event_stream_service: EventStreamService = Depends(event_stream_service),
    auth_info: AuthInfo = Depends(),
    body: EventStreamIn = Body(EventStreamIn(create_raw=True)),
):
    """
    Create Kinesis event stream:
        curl -H "Authorization: bearer $TOKEN" -H "Content-Type: application/json" --data '{"type":"s3"}' -XPOST http://127.0.0.1:8080/{dataset-id}/{version}
    """
    updated_by = auth_info.principal_id
    try:
        event_stream = event_stream_service.create_event_stream(
            dataset_id=dataset_id,
            version=version,
            updated_by=updated_by,
            create_raw=body.create_raw,
        )
    except ResourceConflict:
        response_msg = f"Event stream with id {dataset_id}/{version} already exist"
        raise ErrorResponse(status.HTTP_409_CONFLICT, response_msg)
    except Exception as e:
        logger.exception(e)
        raise ErrorResponse(status.HTTP_500_INTERNAL_SERVER_ERROR, "Server error")

    return event_stream


@router.get(
    "",
    dependencies=[Depends(dataset_owner), Depends(version_exists)],
    response_model=EventStreamWithAcccessRightsOut,
    response_model_by_alias=True,
    responses=error_message_models(
        status.HTTP_404_NOT_FOUND,
    ),
)
def get(dataset=Depends(dataset_exists), event_stream=Depends(get_event_stream)):
    return event_stream.copy(
        update={
            "accessRights": dataset["accessRights"],
            "confidentiality": CONFIDENTIALITY_MAP[dataset["accessRights"]],
        }
    )


@router.put(
    "",
    dependencies=[Depends(dataset_owner), Depends(version_exists)],
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
    response_model=Message,
)
def put():
    raise ErrorResponse(status.HTTP_501_NOT_IMPLEMENTED)


@router.delete(
    "",
    dependencies=[Depends(dataset_owner), Depends(version_exists)],
    response_model=Message,
    responses=error_message_models(
        status.HTTP_404_NOT_FOUND,
        status.HTTP_409_CONFLICT,
        status.HTTP_500_INTERNAL_SERVER_ERROR,
    ),
)
def delete(
    dataset_id: str,
    version: str,
    auth_info: AuthInfo = Depends(),
    event_stream_service=Depends(event_stream_service),
):
    updated_by = auth_info.principal_id
    try:
        event_stream_service.delete_event_stream(
            dataset_id=dataset_id, version=version, updated_by=updated_by
        )
    except ResourceNotFound:
        response_msg = f"Event stream with id {dataset_id}/{version} does not exist"
        raise ErrorResponse(404, response_msg)

    # In the future this endpoint will also delete sub-resources. Until then return 409 if subresources exist. Oyvind Nygard 2020-09-30
    except ResourceConflict:
        response_msg = f"Event stream with id {dataset_id}/{version} contains sub-resources. Delete all related event-sinks and disable event subscription"
        raise ErrorResponse(409, response_msg)
    except Exception as e:
        logger.exception(e)
        raise ErrorResponse(500, "Server error")

    return {"message": f"Deleted event stream with id {dataset_id}/{version}"}
