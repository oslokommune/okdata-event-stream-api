import json
import pytest

from services import SubscribableService, ResourceNotFound

import test.test_data.subscribable as test_data
from .conftest import valid_token


dataset_id = test_data.dataset_id
version = test_data.version
auth_header = {"Authorization": f"bearer {valid_token}"}


def test_get_200(mock_client, mock_subscribable_service, mock_keycloak):
    response = mock_client.get(
        f"/{dataset_id}/{version}/subscribable", headers=auth_header
    )

    assert response.status_code == 200
    assert json.loads(response.data) == json.loads(
        test_data.subscribable_event_stream.subscribable.json(
            exclude={"cf_stack_template"}
        )
    )


def test_get_401_invalid_token(mock_client, mock_keycloak):
    response = mock_client.get(
        f"/{dataset_id}/{version}/subscribable",
        headers={"Authorization": "bearer blablabla"},
    )
    assert response.status_code == 401
    assert json.loads(response.data) == {"message": "Invalid access token"}


def test_get_404_resource_not_found(
    mock_client, mock_subscribable_service_resource_not_found, mock_keycloak
):
    response = mock_client.get(
        f"/{dataset_id}/{version}/subscribable", headers=auth_header
    )
    assert response.status_code == 404
    assert json.loads(response.data) == {
        "message": f"Event stream with id {dataset_id}/{version} does not exist"
    }


@pytest.fixture()
def mock_subscribable_service(monkeypatch, mocker):
    def get_subscribable(self, dataset_id, version):
        return test_data.subscribable_event_stream.subscribable

    monkeypatch.setattr(SubscribableService, "get_subscribable", get_subscribable)

    mocker.spy(SubscribableService, "get_subscribable")


@pytest.fixture()
def mock_subscribable_service_resource_not_found(monkeypatch, mocker):
    def get_subscribable(self, dataset_id, version):
        raise ResourceNotFound

    monkeypatch.setattr(SubscribableService, "get_subscribable", get_subscribable)
