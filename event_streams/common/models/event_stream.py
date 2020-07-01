from typing import List
from datetime import datetime
from pydantic import BaseModel, Field, validator


class Resource(BaseModel):
    type: str
    properties: dict
    tags: dict


class StackTemplate(BaseModel):
    description: str
    resources: List[Resource]

    class Config:
        allow_population_by_field_name = True

        @classmethod
        def alias_generator(cls, string: str) -> str:
            return string.capitalize()


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
