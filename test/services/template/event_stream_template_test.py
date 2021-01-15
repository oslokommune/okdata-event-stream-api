import json
from services.template import EventStreamTemplate
import services.template.stream as stream
import test.test_data.stream as test_data
from test import test_utils

stream.create_pipeline_triggers = True


def test_generate_event_stream_cf_template():
    dataset = {"Id": test_data.dataset_id, "accessRights": test_data.accessRights}
    template = EventStreamTemplate(
        dataset, test_data.version, test_data.updated_by, True
    )
    processed_and_raw_template = template.generate_stack_template()
    test_utils.validate_cf_template(processed_and_raw_template.json())
    assert (
        json.loads(processed_and_raw_template.json())
        == test_data.processed_and_raw_cf_template
    )
    template.create_raw = False
    processed_only_template = template.generate_stack_template()
    test_utils.validate_cf_template(processed_only_template.json())
    assert (
        json.loads(processed_only_template.json()) == test_data.processed_only_template
    )
