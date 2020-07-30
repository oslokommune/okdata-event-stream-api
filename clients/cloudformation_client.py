import os
import boto3


class CloudformationClient:
    def __init__(self):
        sts = boto3.client("sts")
        account_id = sts.get_caller_identity()["Account"]
        region = os.environ["AWS_REGION"]
        self.client = boto3.client("cloudformation", region_name=region)
        self.sns_topic_arn = (
            f"arn:aws:sns:{region}:{account_id}:event-stream-api-cloudformation-events"
        )

    def create_stack(self, name, template, tags):
        self.client.create_stack(
            StackName=name,
            TemplateBody=template,
            Capabilities=["CAPABILITY_NAMED_IAM"],
            NotificationARNs=[self.sns_topic_arn],
            Tags=tags,
        )

    def delete_stack(self, name):
        self.client.delete_stack(StackName=name)
