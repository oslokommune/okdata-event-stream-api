from flask import Flask

from event_streams_api.stream.api import api as stream
from event_streams_api.subscribable.api import api as subscribable
from event_streams_api.sinks.api import api as sinks


app = Flask(__name__)
app.url_map.strict_slashes = False

app.register_blueprint(stream.blueprint)
app.register_blueprint(sinks.blueprint)  # , url_prefix="/sinks")
app.register_blueprint(subscribable.blueprint)
# app.register_blueprint(history.blueprint)
