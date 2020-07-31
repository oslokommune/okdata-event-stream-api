from flask import Flask

from origo.data.dataset import Dataset

from clients import setup_origo_sdk
from resources.api import Api
from resources.routes import initialize_routes
from resources.authorizer import auth


app = Flask(__name__)
app.url_map.strict_slashes = False
api = Api(app)

app.dataset_client = setup_origo_sdk(auth.config, Dataset)

initialize_routes(api)
