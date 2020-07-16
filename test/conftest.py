import pytest
from moto import mock_dynamodb2, mock_cloudformation, mock_sts
from clients import CloudformationClient


@pytest.fixture(scope="function")
def mock_boto(monkeypatch):
    mock_dynamodb2().start()
    mock_cloudformation().start()
    mock_sts().start()

    # There is a bug with moto's mock_cloudformation that gives this error on create_stack():  AttributeError: 'Stream' object has no attribute 'get_cfn_attribute'.
    # Hence this workaround to validate the cloudformation template. Oyvind Nygard 2020-07-13
    def create_stack(self, name, template, tags):
        CloudformationClient().client.validate_template(TemplateBody=template)
        return

    monkeypatch.setattr(CloudformationClient, "create_stack", create_stack)
