import pytest
from unittest.mock import ANY

from services import SubscribableService
from services.exceptions import (
    ResourceNotFound,
    ResourceConflict,
    ParentResourceNotReady,
)

import test.test_data.subscribable as test_data
from .conftest import valid_token, valid_token_no_access, username


dataset_id = test_data.dataset_id
version = test_data.version
auth_header = {"Authorization": f"bearer {valid_token}"}


def test_get_200(mock_client, mock_subscribable_service, mock_keycloak):
    response = mock_client.get(
        f"/{dataset_id}/{version}/subscribable", headers=auth_header
    )

    assert response.status_code == 200
    assert response.json().keys() == {
        "status",
        "updated_by",
        "updated_at",
        "enabled",
    }


def test_get_401_invalid_token(mock_client, mock_keycloak):
    response = mock_client.get(
        f"/{dataset_id}/{version}/subscribable",
        headers={"Authorization": "bearer blablabla"},
    )
    assert response.status_code == 401
    assert response.json() == {"message": "Invalid access token"}


def test_get_404_resource_not_found(
    mock_client, mock_subscribable_service_resource_not_found, mock_keycloak
):
    response = mock_client.get(
        f"/{dataset_id}/{version}/subscribable", headers=auth_header
    )
    assert response.status_code == 404
    assert response.json() == {
        "message": f"Event stream with id {dataset_id}/{version} does not exist"
    }


def test_get_500_server_error(
    mock_client, mock_subscribable_service_server_error, mock_keycloak
):
    response = mock_client.get(
        f"/{dataset_id}/{version}/subscribable", headers=auth_header
    )
    assert response.status_code == 500
    assert response.json() == {"message": "Server error"}


def test_put_200(
    mock_client, mock_subscribable_service, mock_keycloak, mock_authorizer
):
    enabled_response = mock_client.put(
        f"/{dataset_id}/{version}/subscribable",
        headers=auth_header,
        json={"enabled": True},
    )

    SubscribableService.enable_subscription.assert_called_once_with(
        self=ANY, dataset_id=dataset_id, version=version, updated_by=username
    )

    assert enabled_response.status_code == 200
    assert enabled_response.json().keys() == {
        "status",
        "updated_by",
        "updated_at",
        "enabled",
    }

    disabled_response = mock_client.put(
        f"/{dataset_id}/{version}/subscribable",
        headers=auth_header,
        json={"enabled": False},
    )

    SubscribableService.disable_subscription.assert_called_once_with(
        self=ANY, dataset_id=dataset_id, version=version, updated_by=username
    )

    assert disabled_response.status_code == 200
    assert disabled_response.json().keys() == {
        "status",
        "updated_by",
        "updated_at",
        "enabled",
    }


def test_put_401_invalid_token(mock_client, mock_keycloak, mock_authorizer):
    response = mock_client.put(
        f"/{dataset_id}/{version}/subscribable",
        headers={"Authorization": "bearer blablabla"},
    )
    assert response.status_code == 401
    assert response.json() == {"message": "Invalid access token"}


def test_post_403_invalid_token(mock_client, mock_keycloak, mock_authorizer):
    response = mock_client.put(
        f"/{dataset_id}/{version}/subscribable",
        headers={"Authorization": f"bearer {valid_token_no_access}"},
    )
    assert response.status_code == 403
    assert response.json() == {"message": "Forbidden"}


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            {},
            {
                "detail": [
                    {
                        "loc": ["body"],
                        "msg": "field required",
                        "type": "value_error.missing",
                    }
                ]
            },
        ),
        (
            {"json": {"foo": "bar"}},
            {
                "detail": [
                    {
                        "loc": ["body", "enabled"],
                        "msg": "field required",
                        "type": "value_error.missing",
                    }
                ]
            },
        ),
        (
            {"json": {"enabled": "42"}},
            {
                "detail": [
                    {
                        "loc": ["body", "enabled"],
                        "msg": "value could not be parsed to a boolean",
                        "type": "type_error.bool",
                    }
                ]
            },
        ),
    ],
)
def test_put_400_bad_request(
    test_input,
    expected,
    mock_client,
    mock_subscribable_service,
    mock_keycloak,
    mock_authorizer,
):
    response = mock_client.put(
        f"/{dataset_id}/{version}/subscribable", headers=auth_header, **test_input
    )
    assert response.status_code == 422
    assert response.json() == expected


def test_put_404_resource_not_found(
    mock_client,
    mock_subscribable_service_resource_not_found,
    mock_keycloak,
    mock_authorizer,
):
    response = mock_client.put(
        f"/{dataset_id}/{version}/subscribable",
        headers=auth_header,
        json={"enabled": True},
    )
    assert response.status_code == 404
    assert response.json() == {
        "message": f"Event stream with id {dataset_id}/{version} does not exist"
    }


def test_put_409_resource_conflict(
    mock_client,
    mock_subscribable_service_resource_conflict,
    mock_keycloak,
    mock_authorizer,
):
    response = mock_client.put(
        f"/{dataset_id}/{version}/subscribable",
        headers=auth_header,
        json={"enabled": True},
    )
    assert response.status_code == 409
    assert response.json() == {
        "message": f"Event stream with id {dataset_id}/{version} is already subscribable"
    }

    response = mock_client.put(
        f"/{dataset_id}/{version}/subscribable",
        headers=auth_header,
        json={"enabled": False},
    )
    assert response.status_code == 409
    assert response.json() == {
        "message": f"Event stream with id {dataset_id}/{version} is not currently subscribable"
    }


def test_put_409_parent_not_ready_conflict(
    mock_client,
    mock_subscribable_service_parent_not_ready_conflict,
    mock_keycloak,
    mock_authorizer,
):
    response = mock_client.put(
        f"/{dataset_id}/{version}/subscribable",
        headers=auth_header,
        json={"enabled": True},
    )
    assert response.status_code == 409
    assert response.json() == {
        "message": f"Event stream with id {dataset_id}/{version} is not ready"
    }


@pytest.fixture()
def mock_subscribable_service(monkeypatch, mocker):
    def get_subscribable(self, dataset_id, version):
        return test_data.subscribable_event_stream.subscribable

    def enable_subscription(self, dataset_id, version, updated_by):
        return test_data.subscribable_event_stream.subscribable

    def disable_subscription(self, dataset_id, version, updated_by):
        return test_data.event_stream.subscribable

    monkeypatch.setattr(SubscribableService, "get_subscribable", get_subscribable)
    monkeypatch.setattr(SubscribableService, "enable_subscription", enable_subscription)
    monkeypatch.setattr(
        SubscribableService, "disable_subscription", disable_subscription
    )

    mocker.spy(SubscribableService, "enable_subscription")
    mocker.spy(SubscribableService, "disable_subscription")


@pytest.fixture()
def mock_subscribable_service_resource_not_found(monkeypatch):
    def raise_not_found(self, *args, **kwargs):
        raise ResourceNotFound

    monkeypatch.setattr(SubscribableService, "get_subscribable", raise_not_found)
    monkeypatch.setattr(SubscribableService, "enable_subscription", raise_not_found)
    monkeypatch.setattr(SubscribableService, "disable_subscription", raise_not_found)


@pytest.fixture()
def mock_subscribable_service_resource_conflict(monkeypatch):
    def raise_conflict(self, *args, **kwargs):
        raise ResourceConflict

    monkeypatch.setattr(SubscribableService, "enable_subscription", raise_conflict)
    monkeypatch.setattr(SubscribableService, "disable_subscription", raise_conflict)


@pytest.fixture()
def mock_subscribable_service_parent_not_ready_conflict(monkeypatch):
    def raise_conflict(self, *args, **kwargs):
        raise ParentResourceNotReady

    monkeypatch.setattr(SubscribableService, "enable_subscription", raise_conflict)


@pytest.fixture()
def mock_subscribable_service_server_error(monkeypatch):
    def raise_exception(self, dataset_id, version):
        raise Exception

    monkeypatch.setattr(SubscribableService, "get_subscribable", raise_exception)
    monkeypatch.setattr(SubscribableService, "enable_subscription", raise_exception)
    monkeypatch.setattr(SubscribableService, "disable_subscription", raise_exception)
