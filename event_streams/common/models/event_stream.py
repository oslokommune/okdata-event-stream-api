from typing import List
from datetime import datetime
from pydantic import BaseModel


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
    cf_stack_template: StackTemplate
    cf_status: str = "INACTIVE"


class Subscribable(Stack):
    enabled: bool


class Sink(Stack):
    id: str
    type: str
    config: dict


class EventStream(Stack):
    id: str
    create_raw: bool
    updated_by: str
    updated_at: datetime = datetime.now()
    deleted: bool = False
    subscribable: Subscribable = None
    sinks: List[Sink] = list()

    @property
    def is_active(self):
        return self.cf_status == "ACTIVE"


es = EventStream(**{
    "id": "dataset_id/version",
    "cf_stack_template": {
        "resources": [{"type": "foo", "properties": {}, "tags": {}}],
        "description": "Stack template description."
    },
    "cf_status": "CREATE_IN_PROGRESS",
    "subscribable": {
        "enabled": True,
        "cf_stack_template": {
            "resources": [{"type": "foo", "properties": {}, "tags": {}}],
            "description": "Stack template description."
        },
        "cf_status": "active"
    },
    # "sinks": [{
    #     "id": "short-uuid",
    #     "type": "elasticsearch",
    #     "config": {"es_cluster": "some-uri"},
    #     "cf_stack_template": {"foo": "bar"},
    #     "cf_status": "create_in_progress"
    # }],
    "create_raw": True,
    "updated_by": "janedoe"
})
esd = es.dict()
esj = es.json(indent=4, by_alias=True)
ess = es.schema_json(indent=2, by_alias=True)

print("=" * 50)
print(es)
print(esd)
print(esj)
print(es.is_active)
print("-" * 25)
# print(ess)
