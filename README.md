Event streams API
=========================

## Tests

Tests are run using [tox](https://pypi.org/project/tox/): `make test`

For tests and linting we use [pytest](https://pypi.org/project/pytest/),
[flake8](https://pypi.org/project/flake8/) and
[black](https://pypi.org/project/black/).


## Run

`make run` to start up Flask app locally. Binds to port 5000 by default. Change port/environment:

```
export FLASK_ENV=development
export FLASK_RUN_PORT=8080
```


## Deploy

`make deploy` or `make deploy-prod`

Requires `saml2aws`
