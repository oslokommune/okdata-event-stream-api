from resources import (
    StreamResource,
    SubscribableResource,
    SinkResource,
    SinksResource,
    StreamEventResource,
    StreamStatisticsResource,
)


def initialize_routes(api):
    api.add_resource(
        StreamResource,
        "/<string:dataset_id>/<string:version>",
    )
    api.add_resource(
        SinksResource,
        "/<string:dataset_id>/<string:version>/sinks",
    )
    api.add_resource(
        SinkResource,
        "/<string:dataset_id>/<string:version>/sinks/<string:sink_id>",
    )
    api.add_resource(
        SubscribableResource, "/<string:dataset_id>/<string:version>/subscribable"
    )
    api.add_resource(
        StreamEventResource,
        "/<string:dataset_id>/<string:version>/events",
    )
    api.add_resource(
        StreamStatisticsResource,
        "/<string:dataset_id>/<string:version>/events/statistics",
    )
