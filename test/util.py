import boto3


def create_event_stream(stream_name, region="eu-west-1"):
    kinesis = boto3.client("kinesis", region_name=region)
    kinesis.create_stream(StreamName=stream_name, ShardCount=1)
    return kinesis
