APP_NAME=$(shell basename $$PWD)
APP_VERSION?=$(shell git log -1 --format=%h)
ROOT_PATH=$(shell git rev-parse --show-toplevel)

API_PATH=$(ROOT_PATH)/api
PROTO_FILES=$(shell cd $(API_PATH) && find . -name "*.proto")
PROTOC=cd $(API_PATH) && protoc \
	--proto_path=. \
	--proto_path=../third_party \
	--go_out=paths=source_relative:. \
	--go-grpc_out=paths=source_relative:. \
	--validate_out=paths=source_relative,lang=go:. \


.PHONY: api
api:
	$(PROTOC) $(PROTO_FILES)
