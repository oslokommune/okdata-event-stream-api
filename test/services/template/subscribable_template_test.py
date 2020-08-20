import json
from services.template import SubscribableTemplate
from test import test_utils
import test.test_data.subscribable as test_data


def test_generate_subscribable_cf_template():
    dataset = {"Id": test_data.dataset_id, "confidentiality": test_data.confidentiality}
    template = SubscribableTemplate(dataset, test_data.version)
    subscribable_template = template.generate_stack_template()

    test_utils.validate_cf_template(subscribable_template.json())
    assert (
        json.loads(subscribable_template.json()) == test_data.subscribable_cf_template
    )
