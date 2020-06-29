from flask import Blueprint
from flask_restful import Api, Resource


class Api(Api):
    """ Helper abstraction of the Flask-RESTful Api class that
    also configures a Flask blueprint (collection of routes)
    passed to the Api object (instead of an app instance).

    https://flask-restful.readthedocs.io/en/latest/api.html#id1
    https://flask.palletsprojects.com/en/1.1.x/api/#blueprint-objects

    Default ``url_part_order`` for Api is "bae": blueprint/api/endpoint.
    """

    def __init__(self, name, import_name, *args, **kwargs):
        blueprint = Blueprint(
            name=name,
            import_name=import_name,
            # url_prefix="/<string:dataset_id>/<int:version>",
            #    Prefix all "implicitly"?
        )

        # self.url_part_order = "bae" # Default
        # self.prefix = f"/{name}"

        super().__init__(blueprint, *args, **kwargs)


class Resource(Resource):
    # TODO: Implement common methods?

    def get_dataset_or_404(dataset_id, version):
        # Return dataset metadata
        pass
