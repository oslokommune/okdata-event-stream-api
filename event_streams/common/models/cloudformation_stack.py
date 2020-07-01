from typing import List
from pydantic import BaseModel, validator


class BaseStackModel(BaseModel):
    def json(self, *args, **kwargs):
        kwargs["by_alias"] = True
        return super().dict(*args, **kwargs)

    def dict(self, *args, **kwargs):
        kwargs["by_alias"] = True
        return super().dict(*args, **kwargs)

    class Config:
        allow_population_by_field_name = True

        @classmethod
        def alias_generator(cls, string: str) -> str:
            return string.capitalize()


class StackResourceTag(BaseStackModel):
    key: str
    value: str

    @validator("key", "value")
    def check_max_length(cls, value, field):
        max_length = 256 if field.name == "value" else 128
        if len(value) > max_length:
            raise ValueError(f"length of {field.name} cannot exceed {max_length}")
        return value


class StackResource(BaseStackModel):
    type: str
    properties: dict
    tags: List[StackResourceTag]


class StackTemplate(BaseStackModel):
    description: str
    resources: List[StackResource]
