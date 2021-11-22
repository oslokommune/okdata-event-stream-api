from aws_xray_sdk.core import xray_recorder
from moto import mock_kinesis

from resources.events import event_service
from test.util import create_event_stream

xray_recorder.begin_segment("Test")


def test_stream_name(event_streams_table):
    stream_name = event_service()._stream_name("foo", "1", "green")
    assert stream_name == "dp.green.foo.incoming.1.json"


def test_stream_name_raw(event_streams_table):
    event_streams_table.put_item(
        Item={"id": "foo/1", "config_version": 2, "create_raw": True}
    )
    stream_name = event_service()._stream_name("foo", "1", "green")
    assert stream_name == "dp.green.foo.raw.1.json"


def test_event_records():
    event_body = [
        {"key00": "value00", "key01": "value01"},
        {"key10": "value10", "key11": "value11"},
        {"key20": "value20", "key21": "value21"},
        {"key30": "value30", "key31": "value31"},
    ]
    expected = [
        {"PartitionKey": "aa-bb", "Data": '{"key00": "value00", "key01": "value01"}\n'},
        {"PartitionKey": "aa-bb", "Data": '{"key10": "value10", "key11": "value11"}\n'},
        {"PartitionKey": "aa-bb", "Data": '{"key20": "value20", "key21": "value21"}\n'},
        {"PartitionKey": "aa-bb", "Data": '{"key30": "value30", "key31": "value31"}\n'},
    ]

    records = event_service()._event_records(event_body)

    assert all(x["Data"] == y["Data"] for x, y in zip(records, expected))


@mock_kinesis
def test_put_records_to_kinesis():
    create_event_stream("foo")
    record_list = [
        {
            "PartitionKey": "aa-bb",
            "Data": '{"data": {"key30": "value30", "key31": "value31"}, "datasetId": "d123", "version": "1"}',
        }
    ] * 100
    response = event_service()._put_records_to_kinesis(record_list, "foo")

    assert response["FailedRecordCount"] == 0


def test_failed_records():
    put_records_response = {
        "FailedRecordCount": 2,
        "Records": [
            {
                "SequenceNumber": "21269319989900637946712965403778482371",
                "ShardId": "shardId-000000000001",
            },
            {
                "ErrorCode": "ProvisionedThroughputExceededException",
                "ErrorMessage": "Rate exceeded for shard shardId...",
            },
            {
                "SequenceNumber": "21269319989900637946712965403778482371",
                "ShardId": "shardId-000000000001",
            },
            {
                "ErrorCode": "ProvisionedThroughputExceededException",
                "ErrorMessage": "Rate exceeded for shard shardId...",
            },
        ],
    }
    record_list = [
        {"PartitionKey": "aa-bb", "Data": '{"key00": "value00", "key01": "value01"}'},
        {"PartitionKey": "aa-bb", "Data": '{"key10": "value10", "key11": "value11"}'},
        {"PartitionKey": "aa-bb", "Data": '{"key20": "value20", "key21": "value21"}'},
        {"PartitionKey": "aa-bb", "Data": '{"key30": "value30", "key31": "value31"}'},
    ]
    expected = [
        {"PartitionKey": "aa-bb", "Data": '{"key10": "value10", "key11": "value11"}'},
        {"PartitionKey": "aa-bb", "Data": '{"key30": "value30", "key31": "value31"}'},
    ]
    failed_records_list = event_service()._failed_records(
        record_list, put_records_response["Records"]
    )

    assert failed_records_list == expected
