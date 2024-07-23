VENV_BIN ?= python3 -m venv
VENV_DIR ?= .venv
PIP_CMD ?= pip3

STACK_NAME ?= DmsS3ToKinesisStack
BUCKET_NAME ?= source-bucket-s3-kinesis-dms
BUCKET_FOLDER ?= sourceData
CHANGE_DATA ?= changedata
ENDPOINT_URL = http://localhost.localstack.cloud:4566
export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export AWS_DEFAULT_REGION ?= us-east-1

VENV_RUN = . $(VENV_ACTIVATE)

CLOUD_ENV = STACK_NAME=$(STACK_NAME) BUCKET_NAME=$(BUCKET_NAME) BUCKET_FOLDER=$(BUCKET_FOLDER) CHANGE_DATA=$(CHANGE_DATA)
LOCAL_ENV = STACK_NAME=$(STACK_NAME) BUCKET_NAME=$(BUCKET_NAME) BUCKET_FOLDER=$(BUCKET_FOLDER) CHANGE_DATA=$(CHANGE_DATA) ENDPOINT_URL=$(ENDPOINT_URL)

ifeq ($(OS), Windows_NT)
	VENV_ACTIVATE = $(VENV_DIR)/Scripts/activate
else
	VENV_ACTIVATE = $(VENV_DIR)/bin/activate
endif

usage:                    ## Show this help
	@grep -Fh "##" $(MAKEFILE_LIST) | grep -Fv fgrep | sed -e 's/:.*##\s*/##/g' | awk -F'##' '{ printf "%-25s %s\n", $$1, $$2 }'

$(VENV_ACTIVATE):
	test -d $(VENV_DIR) || $(VENV_BIN) $(VENV_DIR)
	$(VENV_RUN); touch $(VENV_ACTIVATE)

venv: $(VENV_ACTIVATE)    ## Create a new (empty) virtual environment

start:
	$(LOCAL_ENV) docker compose up --build --detach --wait

install: venv
	$(VENV_RUN); $(PIP_CMD) install -r requirements.txt

deploy:
	$(VENV_RUN); $(LOCAL_ENV) cdklocal bootstrap --output ./cdk.local.out
	$(VENV_RUN); $(LOCAL_ENV) cdklocal deploy --require-approval never --output ./cdk.local.out

deploy-aws:
	$(VENV_RUN); $(CLOUD_ENV) cdk bootstrap
	$(VENV_RUN); $(CLOUD_ENV) cdk deploy --require-approval never

destroy:
	docker-compose down

destroy-aws: venv
	$(VENV_RUN); $(CLOUD_ENV) cdk destroy --require-approval never

run:
	$(VENV_RUN); $(LOCAL_ENV) python run.py

run-aws:
	$(VENV_RUN); $(CLOUD_ENV) python run.py
