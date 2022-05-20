.DEV_PROFILE := okdata-dev
.PROD_PROFILE := okdata-prod

PY_VERSION := 3
GLOBAL_PY := python$(PY_VERSION)
BUILD_VENV ?= .build_venv
BUILD_PY := $(BUILD_VENV)/bin/python

.PHONY: init
init: node_modules $(BUILD_VENV)

node_modules: package.json package-lock.json
	npm install

$(BUILD_VENV):
	$(GLOBAL_PY) -m venv $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U pip
	$(BUILD_PY) -m pip install -r requirements.txt

.PHONY: format
format: $(BUILD_VENV)/bin/black
	$(BUILD_PY) -m black .

.PHONY: test
test: $(BUILD_VENV)/bin/tox
	$(BUILD_PY) -m tox -p auto -o

.PHONY: run
run: $(BUILD_VENV)/bin/uvicorn
	$(BUILD_VENV)/bin/uvicorn app:app --reload

.PHONY: upgrade-deps
upgrade-deps: $(BUILD_VENV)/bin/pip-compile
	$(BUILD_VENV)/bin/pip-compile -U

.PHONY: deploy
deploy: login-dev init format test
	@echo "\nDeploying to stage: dev\n"
	sls deploy --stage dev --aws-profile $(.DEV_PROFILE)

.PHONY: deploy-prod
deploy-prod: login-prod init format is-git-clean test
	sls deploy --stage prod --aws-profile $(.PROD_PROFILE)

.PHONY: undeploy
undeploy: login-dev init
	@echo "\nUndeploying stage: dev\n"
	sls remove --stage dev --aws-profile $(.DEV_PROFILE)

.PHONY: undeploy-prod
undeploy-prod: login-prod init
	@echo "\nUndeploying stage: prod\n"
	sls remove --stage prod --aws-profile $(.PROD_PROFILE)

.PHONY: login-dev
login-dev:
ifndef OKDATA_AWS_ROLE_DEV
	$(error OKDATA_AWS_ROLE_DEV is not set)
endif
	saml2aws login --role=$(OKDATA_AWS_ROLE_DEV) --profile=$(.DEV_PROFILE)

.PHONY: login-prod
login-prod:
ifndef OKDATA_AWS_ROLE_PROD
	$(error OKDATA_AWS_ROLE_PROD is not set)
endif
	saml2aws login --role=$(OKDATA_AWS_ROLE_PROD) --profile=$(.PROD_PROFILE)

.PHONY: is-git-clean
is-git-clean:
	@status=$$(git fetch origin && git status -s -b) ;\
	if test "$${status}" != "## main...origin/main"; then \
		echo; \
		echo Git working directory is dirty, aborting >&2; \
		false; \
	fi

.PHONY: build
build: $(BUILD_VENV)/bin/wheel $(BUILD_VENV)/bin/twine
	$(BUILD_PY) setup.py sdist bdist_wheel

###
# Python build dependencies
##

$(BUILD_VENV)/bin/pip-compile: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U pip-tools

$(BUILD_VENV)/bin/%: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U $*

$(BUILD_VENV)/lib/python$(PY_VERSION)/site-packages/%: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U $*
