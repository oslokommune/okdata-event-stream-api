from typing import List, Dict
from pydantic import BaseModel, Field


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
    key: str = Field(max_length=128)
    value: str = Field(max_length=256)


class StackResource(BaseStackModel):
    type: str
    properties: dict
    tags: List[StackResourceTag]


class StackTemplate(BaseStackModel):
    description: str
    resources: Dict[str, StackResource]
