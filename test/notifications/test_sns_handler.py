import notifications.sns_handler as sns_handler
import pytest
from unittest.mock import ANY
from services import CfStatusService

stack_name = "event-stream-dataset-id-1"
message_create_complete = f"StackId='arn:aws:cloudformation:eu-west-1:123456789000:stack/{stack_name}/c4387bb0-37d1-11ea-9fc9-0a1dada20d7a'\nTimestamp='2020-01-15T20:01:06.861Z'\nEventId='c4396610-37d1-11ea-9fc9-0a1dada20d7a'\nLogicalResourceId='{stack_name}'\nNamespace='123456789000'\nPhysicalResourceId='arn:aws:cloudformation:eu-west-1:123456789000:stack/{stack_name}/c4387bb0-37d1-11ea-9fc9-0a1dada20d7a'\nPrincipalId='***REMOVED***:stream-manager-dev-create-stream'\nResourceStatus='CREATE_COMPLETE'\nResourceStatusReason='User Initiated'\nResourceType='AWS::CloudFormation::Stack'\nStackName='{stack_name}'\nClientRequestToken='null'\n"
message_delete_complete = f"StackId='arn:aws:cloudformation:eu-west-1:123456789000:stack/{stack_name}/c4387bb0-37d1-11ea-9fc9-0a1dada20d7a'\nTimestamp='2020-01-15T20:01:06.861Z'\nEventId='c4396610-37d1-11ea-9fc9-0a1dada20d7a'\nLogicalResourceId='{stack_name}'\nNamespace='123456789000'\nPhysicalResourceId='arn:aws:cloudformation:eu-west-1:123456789000:stack/{stack_name}/c4387bb0-37d1-11ea-9fc9-0a1dada20d7a'\nPrincipalId='***REMOVED***:stream-manager-dev-create-stream'\nResourceStatus='DELETE_COMPLETE'\nResourceStatusReason='User Initiated'\nResourceType='AWS::CloudFormation::Stack'\nStackName='{stack_name}'\nClientRequestToken='null'\n"
message_rollback_complete = f"StackId='arn:aws:cloudformation:eu-west-1:123456789000:stack/{stack_name}/c4387bb0-37d1-11ea-9fc9-0a1dada20d7a'\nTimestamp='2020-01-15T20:01:06.861Z'\nEventId='c4396610-37d1-11ea-9fc9-0a1dada20d7a'\nLogicalResourceId='{stack_name}'\nNamespace='123456789000'\nPhysicalResourceId='arn:aws:cloudformation:eu-west-1:123456789000:stack/{stack_name}/c4387bb0-37d1-11ea-9fc9-0a1dada20d7a'\nPrincipalId='***REMOVED***:stream-manager-dev-create-stream'\nResourceStatus='ROLLBACK_COMPLETE'\nResourceStatusReason='User Initiated'\nResourceType='AWS::CloudFormation::Stack'\nStackName='{stack_name}'\nClientRequestToken='null'\n"


def generate_sns_event(message):
    return {"Records": [{"Sns": {"Message": message}}]}


def test_handle_create_complete(mock_cf_status_service):

    sns_handler.handle(generate_sns_event(message_create_complete), {})
    CfStatusService.update_status.assert_called_once_with(
        self=ANY, stack_name=stack_name, cf_status="ACTIVE"
    )


def test_handle_rollback_complete(mock_cf_status_service):

    sns_handler.handle(generate_sns_event(message_rollback_complete), {})
    CfStatusService.update_status.assert_called_once_with(
        self=ANY, stack_name=stack_name, cf_status="OPERATION_FAILED"
    )


def test_handle_delete_complete(mock_cf_status_service):

    sns_handler.handle(generate_sns_event(message_delete_complete), {})
    CfStatusService.update_status.assert_called_once_with(
        self=ANY, stack_name=stack_name, cf_status="INACTIVE"
    )


@pytest.fixture()
def mock_cf_status_service(monkeypatch, mocker):
    def update_status(self, stack_name, cf_status):
        return

    monkeypatch.setattr(CfStatusService, "update_status", update_status)
    mocker.spy(CfStatusService, "update_status")
