from datetime import datetime
from typing import List
from pydantic import BaseModel, Field, validator

# from pydantic.generics import GenericModel

from .cloudformation_stack import StackTemplate


class Stack(BaseModel):
    cf_stack_template: StackTemplate  # https://pydantic-docs.helpmanual.io/usage/types/#json-type (?)
    cf_status: str = "INACTIVE"

    @validator("cf_status")
    def must_be_uppercase(cls, v):
        return v.upper()


class Subscribable(Stack):
    enabled: bool


class Sink(Stack):
    id: str  # ShortUUID = Field(default_factory=ShortUUID...)
    type: str
    config: dict


class EventStream(Stack):
    id: str
    create_raw: bool
    updated_by: str
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    deleted: bool = False
    subscribable: Subscribable = None
    sinks: List[Sink] = list()

    @property
    def is_active(self):
        return self.cf_status == "ACTIVE"

    @property
    def cf_stack_name(self):
        [dataset_id, version] = id.split("/")
        return f"stream-manager-{dataset_id}-{version}"
