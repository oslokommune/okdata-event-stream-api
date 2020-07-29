import json
import pytest

from services import SubscribableService, ResourceNotFound

from test import test_utils
import test.test_data.subscribable as test_data


def test_get_subscribable(mock_boto):
    test_utils.create_event_streams_table(
        item_list=[json.loads(test_data.subscribable_event_stream.json())]
    )

    subscribable_service = SubscribableService()

    subscribable = subscribable_service.get_subscribable(
        test_data.dataset_id, test_data.version
    )

    assert subscribable == test_data.subscribable_event_stream.subscribable

    with pytest.raises(ResourceNotFound):
        subscribable_service.get_subscribable(test_data.dataset_id, 2)
