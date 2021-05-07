import logging
from typing import List, Optional
from datetime import datetime
from fastapi import Depends, APIRouter, status
from pydantic import BaseModel, Field

from resources.authorizer import AuthInfo, authorize, version_exists
from resources.origo_clients import dataset_client
from resources.errors import ErrorResponse, error_message_models
from services import (
    SinkService,
    ResourceConflict,
    ResourceNotFound,
    ResourceUnderDeletion,
    ResourceUnderConstruction,
    SubResourceNotFound,
)

logger = logging.getLogger()

router = APIRouter()


def sink_service(dataset_client=Depends(dataset_client)) -> SinkService:
    return SinkService(dataset_client)


class SinkIn(BaseModel):
    type: str


class SinkOut(SinkIn):
    cf_status: str = Field("INACTIVE", max_length=20, alias="status")
    updated_by: Optional[str]
    updated_at: datetime


@router.get(
    "/{sink_type}",
    dependencies=[
        Depends(authorize("okdata:dataset:update")),
        Depends(version_exists),
    ],
    response_model=SinkOut,
    response_model_by_alias=True,
    responses=error_message_models(400, 404, 500),
)
def get(
    dataset_id: str,
    version: str,
    sink_type: str,
    sink_service=Depends(sink_service),
):
    try:
        return sink_service.get_sink(dataset_id, version, sink_type)
    except KeyError as e:
        response_msg = f"Invalid sink type: {sink_type}"
        logger.exception(e)
        raise ErrorResponse(400, message=response_msg)
    except SubResourceNotFound:
        response_msg = (
            f"Sink of type {sink_type} does not exist on {dataset_id}/{version}"
        )
        raise ErrorResponse(404, message=response_msg)
    except Exception as e:
        response_msg = f"Could not get sink of type {sink_type} from event stream {dataset_id}/{version}"
        logger.exception(e)
        raise ErrorResponse(500, message=response_msg)


@router.delete(
    "/{sink_type}",
    dependencies=[
        Depends(authorize("okdata:dataset:update")),
        Depends(version_exists),
    ],
    responses=error_message_models(400, 404, 409, 500),
)
def delete(
    dataset_id: str,
    version: str,
    sink_type: str,
    auth_info: AuthInfo = Depends(),
    sink_service=Depends(sink_service),
):
    updated_by = auth_info.principal_id
    try:
        sink_service.disable_sink(dataset_id, version, sink_type, updated_by)
    except KeyError as e:
        response_msg = f"Invalid sink type: {sink_type}"
        logger.exception(e)
        raise ErrorResponse(400, response_msg)
    except ResourceNotFound:
        response_msg = f"Event stream with id {dataset_id}/{version} does not exist"
        raise ErrorResponse(404, response_msg)
    except SubResourceNotFound:
        response_msg = (
            f"Sink of type {sink_type} does not exist on {dataset_id}/{version}"
        )
        raise ErrorResponse(404, response_msg)
    except ResourceUnderConstruction:
        response_msg = (
            f"Sink of type {sink_type} cannot be disabled since it is being constructed"
        )
        raise ErrorResponse(409, response_msg)
    except Exception as e:
        logger.exception(e)
        raise ErrorResponse(500, message="Server error")

    return {
        "message": f"Disabled sink of type {sink_type} for stream {dataset_id}/{version}"
    }


@router.post(
    "",
    dependencies=[
        Depends(authorize("okdata:dataset:update")),
        Depends(version_exists),
    ],
    response_model=SinkOut,
    response_model_by_alias=True,
    status_code=status.HTTP_201_CREATED,
    responses=error_message_models(400, 409, 404, 500),
)
def post(
    body: SinkIn,
    dataset_id: str,
    version: str,
    auth_info: AuthInfo = Depends(),
    sink_service=Depends(sink_service),
):
    try:
        return sink_service.enable_sink(
            dataset_id, version, body.type, auth_info.principal_id
        )
    except KeyError as e:
        response_msg = f"Invalid sink type: {body.type}"
        logger.exception(e)
        raise ErrorResponse(400, response_msg)
    except ResourceConflict as rc:
        response_msg = str(rc)
        logger.exception(rc)
        raise ErrorResponse(409, response_msg)
    except ResourceUnderDeletion:
        response_msg = f"Cannot create sink since a sink of type {body.type} currently is being deleted"
        raise ErrorResponse(409, response_msg)
    except ResourceNotFound as e:
        response_msg = f"Event stream {dataset_id}/{version} does not exist"
        logger.exception(e)
        raise ErrorResponse(404, response_msg)
    except Exception as e:
        response_msg = f"Could not update event stream: {str(e)}"
        logger.exception(e)
        raise ErrorResponse(500, response_msg)


@router.get(
    "",
    dependencies=[
        Depends(authorize("okdata:dataset:update")),
        Depends(version_exists),
    ],
    response_model=List[SinkOut],
    response_model_by_alias=True,
    responses=error_message_models(500),
)
def list_sinks(dataset_id: str, version: str, sink_service=Depends(sink_service)):
    try:
        sinks = sink_service.get_sinks(dataset_id, version)
        return list(filter(lambda sink: not sink.deleted, sinks))
    except Exception as e:
        response_msg = f"Could not get sink list: {str(e)}"
        logger.exception(e)
        raise ErrorResponse(500, response_msg)
