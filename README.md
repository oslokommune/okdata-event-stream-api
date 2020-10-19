Event stream API
=========================

REST API for:
* [Creating and managing event streams](#creating-and-managing-event-streams)
* Sending data to event streams

## Tests

Tests are run using [tox](https://pypi.org/project/tox/): `make test`

For tests and linting we use [pytest](https://pypi.org/project/pytest/),
[flake8](https://pypi.org/project/flake8/) and
[black](https://pypi.org/project/black/).

## Setup

`make init`

## Run

Login to aws:
`make login-dev`

Set necessary environment variables:
```
export AWS_PROFILE=saml-origo-dev
export AWS_REGION=eu-west-1
export ORIGO_ENVIRONMENT=dev
```

Start up Flask app locally. Binds to port 5000 by default:
```
make run
```
Note: `make init` will not install the boto3 library, since this dependency is already installed on the server. 
Therefore you must either run `make test` (which installs boto3) or run `.build_venv/bin/python -m pip install boto3` before 
`make run`

Change port/environment:
```
export FLASK_ENV=development
export FLASK_RUN_PORT=8080
```


## Deploy

`make deploy` or `make deploy-prod`

Requires `saml2aws`


## Creating and managing event streams

### curl commands for the API

Create a new event stream: `curl -H "Authorization: bearer $TOKEN" -H "Content-Type: application/json" --data '{}' -XPOST http://127.0.0.1:8080/{dataset-id}/{version}`

Enable an event sink: `curl -H "Authorization: bearer $TOKEN" -H "Content-Type: application/json" --data '{"type":"s3"}' -XPOST http://127.0.0.1:8080/{dataset-id}/{version}/sinks`

Get all sinks: `curl -H "Authorization: bearer $TOKEN" -XGET http://127.0.0.1:8080/{dataset-id}/{version}/sinks`

Get a single sink: `curl -H "Authorization: bearer $TOKEN" -XGET http://127.0.0.1:8080/{dataset-id}/{version}/sinks/{sink_type}`

Disable an event sink: `curl -H "Authorization: bearer $TOKEN" -H "Content-Type: application/json" -XDELETE http://127.0.0.1:8080/{dataset-id}/{version}/sinks/{sink_type}`


## Terminology and resource definitions

### Stream resource

This is the base resource of your event stream. In other words the **Stream** resource can be regarded as the event stream whilst the [Subscribable](#subscribable) and [Sink](#sink) resources can be regarded as
features on the event stream. The Stream's Cloud Formation stack contains the following resources:

* **RawDataStream**: Kinesis data stream `dp.{confidentiality}.{dataset_id}.raw.{version}.json`.
* **RawPipelineTrigger**: Lambda event source mapping from **RawDataStream** to [pipeline-router](https://github.oslo.kommune.no/origo-dataplatform/pipeline-router).
* **ProcessedDataStream**: Kinesis data stream `dp.{confidentiality}.{dataset_id}.processed.{version}.json`.
* **ProcessedPipelineTrigger**: Lambda event source mapping from **ProcessedDataStream** to [pipeline-router](https://github.oslo.kommune.no/origo-dataplatform/pipeline-router).

### Subscribable resource

The Subscribable resource can be regarded as a feature on the event stream that can either be enabled or disabled. If enabled, you connect to [event-data-subscription](https://github.oslo.kommune.no/origo-dataplatform/event-data-subscription) websocket API
and listen to events on your event stream. The subscribable's Cloud Formation stack consists of the following AWS resources:

* **SubscriptionSource**: Lambda event source mapping from **ProcessedDataStream** to [event-data-subscription](https://github.oslo.kommune.no/origo-dataplatform/event-data-subscription).

### Sink resource

The Sink resources can be regarded as destinations that your event stream writes to and entities with access can read from.
So far we have two different event-sinks.

#### S3 sink

The S3 sink's Cloud Formation stack contains the following AWS resources:

* **SinkS3Resource**: Kinesis firehose delivery stream with source=**ProcessedDataStream** and destination=S3.
* **SinkS3ResourceIAM**: Iam role for consuming data from **ProcessedDataStream** and writing objects to S3. The role is used by **SinkS3Resource**.

#### Elasticsearch sink

The Elasticsearch sink's Cloud Formation stack contains the following AWS resources:

* **SinkElasticsearchResource**: Kinesis firehose delivery stream with source=**ProcessedDataStream**, destination=S3 and S3 backup for failed documents.
* **SinkElasticsearchResourceIAM**: Iam role for consuming data from **ProcessedDataStream** and posting to ES(elastic search). The role is used by **SinkElasticsearchResource**.
* **SinkElasticsearchS3BackupResourceIAM**: IAM role for writing objects to S3. The role is used by **SinkElasticsearchResource**.

#### Get historical data

When an Elasticsearch sink is enabled and when data is stored(not backward compatible ), you can access data in a given date through: `{url}/streams/{dataset-id}/{version}/events?from_date={from_date}&to_date={to_date}`
* Example prod: `https://api.data.oslo.systems/streams/renovasjonsbiler-status/1/events?from_date=2020-10-18&to_date=2020-10-19`