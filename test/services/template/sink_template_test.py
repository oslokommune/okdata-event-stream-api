import json
from services.template import SinkTemplate
from database import Sink
from test import test_utils
import test.test_data.sink as test_data

dataset = {"Id": test_data.dataset_id, "accessRights": test_data.access_rights}


def test_generate_sink_elasticsearch_cf_template():
    sink = Sink(type="elasticsearch")
    sink.id = test_data.elasticsearch_sink_id
    template = SinkTemplate(test_data.event_stream, dataset, test_data.version, sink)
    elasticsearch_template = template.generate_stack_template()
    test_utils.validate_cf_template(elasticsearch_template.json())
    assert (
        json.loads(elasticsearch_template.json())
        == test_data.sink_elasticsearch_cf_template
    )


def test_generate_sink_s3_cf_template():
    sink = Sink(type="s3")
    sink.id = test_data.s3_sink_id
    template = SinkTemplate(test_data.event_stream, dataset, test_data.version, sink)
    s3_template = template.generate_stack_template()
    test_utils.validate_cf_template(s3_template.json())
    assert json.loads(s3_template.json()) == test_data.sink_s3_cf_template


def test_really_long_role_name():
    dataset = {
        "Id": "a-really-long-dataset-id-to-test-our-limits-and-beyond",
        "accessRights": test_data.access_rights,
    }
    sink = Sink(type="elasticsearch")
    sink.id = test_data.elasticsearch_sink_id
    template = SinkTemplate(test_data.event_stream, dataset, test_data.version, sink)
    role_name = template.get_iam_role_name(key="es")

    assert (
        role_name == "stream-a-really-long-dataset-id-to-test-our-limits-an-1-abcde-es"
    )
