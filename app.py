from flask import Flask

from flask_restful import Api
from resources.routes import initialize_routes


app = Flask(__name__)
app.url_map.strict_slashes = False
api = Api(app)

initialize_routes(api)
