from functools import wraps
from flask import g, request

# from flask_restful import abort


def requires_auth(method):
    @wraps(method)
    def decorated_function(*args, **kwargs):
        webhook_token = request.args.get("webhook_token")
        header = request.headers.get("Authorization")
        _, bearer_token = header.split() if header else None, None
        print(f"webhook={webhook_token}, bearer={bearer_token}")
        g.principal_id = "janedone"
        return method(*args, **kwargs)

    return decorated_function


def requires_dataset_ownership(method):
    @wraps(method)
    def decorated_function(*args, **kwargs):
        print(f"principal_id={g.principal_id}")
        # abort(403)
        return method(*args, **kwargs)

    return decorated_function
