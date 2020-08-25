from typing import List
from datetime import datetime
from shortuuid import ShortUUID
from pydantic import BaseModel, Field, validator
from typing import Dict
from enum import Enum

from services import datetime_utils


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


class StackTemplate(BaseStackModel):
    description: str
    resources: Dict[str, dict]


class Stack(BaseModel):
    cf_stack_template: StackTemplate = None
    cf_status: str = Field("INACTIVE", max_length=20)
    cf_stack_name: str = None
    updated_by: str = None  # Remove None when all DynamoDB in dev have this field ;)
    updated_at: datetime = Field(
        default_factory=datetime_utils.utc_now_with_timezone, str=datetime.isoformat
    )

    @property
    def is_active(self):
        return self.cf_status == "ACTIVE"

    @validator("cf_status", allow_reuse=True)
    def make_uppercase(cls, v):
        return v.upper()

    # TODO: Ensure that template is set on status change?
    # @validator("cf_status", allow_reuse=True)
    # def ensure_template(cls, v, values):
    #     if v != "INACTIVE" and not values["cf_stack_template"]:
    #         raise ValueError('template must be set')
    #     return v

    class Config:
        validate_assignment = True


class Subscribable(Stack):
    enabled: bool = False

    def get_stack_name(self, dataset_id, version):
        return f"{CfStackType.SUBSCRIBABLE.value}-{dataset_id}-{version}"


class Sink(Stack):
    type: str
    config: dict = {}
    deleted: bool = False
    id: str = Field(default_factory=lambda: ShortUUID().random(length=5).lower())

    def get_stack_name(self, dataset_id, version):
        return f"{CfStackType.SINK.value}-{dataset_id}-{version}-{self.id}"


class SinkType(Enum):
    S3 = "s3"
    ELASTICSEARCH = "elasticsearch"


class EventStream(Stack):
    id: str
    config_version: int = 1
    create_raw: bool
    deleted: bool = False
    subscribable: Subscribable = Field(default_factory=Subscribable)
    sinks: List[Sink] = list()

    def get_stack_name(self):
        [dataset_id, version] = self.id.split("/")
        return f"{CfStackType.EVENT_STREAM.value}-{dataset_id}-{version}"


class CfStackType(Enum):
    EVENT_STREAM = "event-stream"
    SUBSCRIBABLE = "event-subscribable"
    SINK = "event-sink"
