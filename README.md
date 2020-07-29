Event streams API
=========================

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
