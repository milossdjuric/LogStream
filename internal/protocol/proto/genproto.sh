#!/bin/bash

PROTOC_GEN_GO="/home/milossdjuric/go/bin/protoc-gen-go"

protoc --proto_path=./ \
    --go_out=../ \
    --go_opt=paths=source_relative \
    --plugin=protoc-gen-go=$PROTOC_GEN_GO \
    messages.proto