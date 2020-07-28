from flask import make_response
from flask_restful import abort

from database import EventStreamsTable
from resources import Resource
from resources.authorizer import auth


class SubscribableResource(Resource):
    def __init__(self):
        # self.event_stream_service = EventStreamService(current_app.dataset_client)
        self.event_streams_table = EventStreamsTable()

    @auth.accepts_token
    def get(self, dataset_id, version):
        event_stream_id = f"{dataset_id}/{version}"
        event_stream = self.event_streams_table.get_event_stream(event_stream_id)

        if not event_stream:
            abort(404, message=f"Event stream with id {event_stream_id} does not exist")

        return make_response(
            event_stream.subscribable.json(exclude={"cf_stack_template"}), 200
        )

    def put(self, dataset_id, version):
        abort(501)
