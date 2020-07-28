from services import CfStatusService


def handle(event, context):
    cf_status_service = CfStatusService()
    for cloudformation_event in event["Records"]:
        event_message = cloudformation_event_message_to_dict(
            cloudformation_event["Sns"]["Message"]
        )

        stack_name = event_message["StackName"]
        resource_status = event_message["ResourceStatus"]
        resource_type = event_message["ResourceType"]

        if (
            resource_status == "CREATE_COMPLETE"
            and resource_type == "AWS::CloudFormation::Stack"
        ):
            cf_status_service.update_status(stack_name, "ACTIVE")

        elif (
            resource_status == "ROLLBACK_COMPLETE"
            and resource_type == "AWS::CloudFormation::Stack"
        ):
            cf_status_service.update_status(stack_name, "OPERATION_FAILED")

        elif (
            resource_status == "DELETE_COMPLETE"
            and resource_type == "AWS::CloudFormation::Stack"
        ):
            cf_status_service.update_status(stack_name, "INACTIVE")


def cloudformation_event_message_to_dict(message):
    return {
        key: value.strip("'")
        for (key, value) in map(
            lambda line: line.split("=", 1),
            filter(lambda line: bool(line), message.split("\n")),
        )
    }
