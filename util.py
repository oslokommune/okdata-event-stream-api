import os

CONFIDENTIALITY_MAP = {
    "public": "green",
    "restricted": "yellow",
    "non-public": "red",
}


def get_confidentiality(dataset):
    return CONFIDENTIALITY_MAP[dataset["accessRights"]]


def getenv(name):
    """Return the environment variable named `name`, or raise OSError if unset."""
    env = os.getenv(name)

    if not env:
        raise OSError(f"Environment variable {name} is not set")

    return env
