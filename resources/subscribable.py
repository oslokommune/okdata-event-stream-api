import logging
from typing import Optional
from datetime import datetime
from fastapi import Depends, APIRouter
from pydantic import BaseModel, Field

from resources.authorizer import AuthInfo, dataset_owner
from resources.origo_clients import dataset_client
from resources.errors import ErrorResponse, error_message_models
from services import SubscribableService
from services.exceptions import (
    ResourceNotFound,
    ResourceConflict,
    ParentResourceNotReady,
)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

router = APIRouter()


def subscribable_service(dataset_client=Depends(dataset_client)) -> SubscribableService:
    return SubscribableService(dataset_client)


class SubscribableIn(BaseModel):
    enabled: bool


class SubscribableOut(SubscribableIn):
    cf_status: str = Field("INACTIVE", max_length=20, alias="status")
    updated_by: Optional[str]
    updated_at: datetime


@router.get(
    "",
    dependencies=[Depends(AuthInfo)],
    response_model=SubscribableOut,
    response_model_by_alias=True,
    responses=error_message_models(404, 500),
)
def get(
    dataset_id: str, version: str, subscribable_service=Depends(subscribable_service)
):
    try:
        return subscribable_service.get_subscribable(dataset_id, version)
    except ResourceNotFound:
        raise ErrorResponse(
            404,
            f"Event stream with id {dataset_id}/{version} does not exist",
        )
    except Exception as e:
        logger.exception(e)
        raise ErrorResponse(500, "Server error")


@router.put(
    "",
    dependencies=[Depends(dataset_owner)],
    response_model=SubscribableOut,
    response_model_by_alias=True,
    responses=error_message_models(404, 409, 500),
)
def put(
    body: SubscribableIn,
    dataset_id: str,
    version: str,
    auth_info: AuthInfo = Depends(),
    subscribable_service=Depends(subscribable_service),
):
    service_call = (
        subscribable_service.enable_subscription
        if body.enabled
        else subscribable_service.disable_subscription
    )

    try:
        return service_call(dataset_id, version, auth_info.principal_id)
    except ResourceNotFound:
        raise ErrorResponse(
            404,
            f"Event stream with id {dataset_id}/{version} does not exist",
        )
    except ParentResourceNotReady:
        raise ErrorResponse(
            409,
            f"Event stream with id {dataset_id}/{version} is not ready",
        )
    except ResourceConflict:
        subscribable_state = (
            "already subscribable" if body.enabled else "not currently subscribable"
        )
        raise ErrorResponse(
            409,
            f"Event stream with id {dataset_id}/{version} is {subscribable_state}",
        )
    except Exception as e:
        logger.exception(e)
        raise ErrorResponse(500, "Server error")
