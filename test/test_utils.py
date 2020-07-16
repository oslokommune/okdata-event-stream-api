import boto3
from clients import CloudformationClient


def validate_cf_template(cf_template):
    CloudformationClient().client.validate_template(TemplateBody=cf_template)


def create_event_streams_table(item_list=[], region="eu-west-1"):
    table_name = "event-streams"
    client = boto3.client("dynamodb", region_name="eu-west-1")
    client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "event_stream_version", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "event_stream_version", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by_id",
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1,
                },
            },
        ],
    )

    table = boto3.resource("dynamodb", region_name=region).Table(table_name)
    for item in item_list:
        table.put_item(Item=item)

    return table
