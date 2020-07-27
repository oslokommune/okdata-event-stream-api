import os
import pytest
import boto3
from moto import mock_dynamodb2, mock_cloudformation, mock_sts, mock_ssm
from clients import CloudformationClient


@pytest.fixture
def mock_boto(monkeypatch):
    mock_dynamodb2().start()
    mock_cloudformation().start()
    mock_sts().start()
    mock_ssm().start()

    # There is a bug with moto's mock_cloudformation that gives this error on create_stack():  AttributeError: 'Stream' object has no attribute 'get_cfn_attribute'.
    # Hence this workaround to validate the cloudformation template. Oyvind Nygard 2020-07-13
    def create_stack(self, name, template, tags):
        CloudformationClient().client.validate_template(TemplateBody=template)
        return

    monkeypatch.setattr(CloudformationClient, "create_stack", create_stack)

    # Add required values to parameter_store
    initialize_parameter_store()


def initialize_parameter_store():
    ssm_client = boto3.client("ssm", region_name=os.environ["AWS_REGION"])
    ssm_client.put_parameter(
        Name="/dataplatform/shared/keycloak-server-url",
        Description="A test parameter",
        Value="https://test.com/auth/",
        Type="String",
    )
    ssm_client.put_parameter(
        Name="/dataplatform/shared/keycloak-realm",
        Description="A test parameter",
        Value="api-catalog",
        Type="String",
    )
    ssm_client.put_parameter(
        Name="/dataplatform/event-stream-api/keycloak-client-secret",
        Description="A test parameter",
        Value="supersecretpassword",
        Type="SecureString",
    )
