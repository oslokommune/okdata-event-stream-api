from typing import List
from datetime import datetime
from shortuuid import ShortUUID
from pydantic import BaseModel, Field, validator
from typing import Dict

# from pydantic.generics import GenericModel


class BaseStackModel(BaseModel):
    def json(self, *args, **kwargs):
        kwargs["by_alias"] = True
        return super().json(*args, **kwargs)

    def dict(self, *args, **kwargs):
        kwargs["by_alias"] = True
        return super().dict(*args, **kwargs)

    class Config:
        allow_population_by_field_name = True

        @classmethod
        def alias_generator(cls, string: str) -> str:
            return string.capitalize()


class StackResourceTag(BaseStackModel):
    key: str = Field(max_length=128)
    value: str = Field(max_length=256)


class StackResource(BaseStackModel):
    type: str
    properties: dict


class StackTemplate(BaseStackModel):
    description: str
    resources: Dict[str, StackResource]


class Stack(BaseModel):
    cf_stack_template: StackTemplate
    cf_status: str = "INACTIVE"

    @validator("cf_status", allow_reuse=True)
    def make_uppercase(cls, v):
        return v.upper()


class Subscribable(Stack):
    enabled: bool


class Sink(Stack):
    type: str
    config: dict
    id: str = Field(default_factory=lambda: ShortUUID().random(length=5))


class EventStream(Stack):
    id: str
    create_raw: bool
    updated_by: str
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, str=datetime.isoformat
    )
    deleted: bool = False
    subscribable: Subscribable = None
    sinks: List[Sink] = list()

    @property
    def is_active(self):
        return self.cf_status == "ACTIVE"

    @property
    def cf_stack_name(self):
        [dataset_id, version] = self.id.split("/")
        return f"stream-manager-{dataset_id}-{version}"
