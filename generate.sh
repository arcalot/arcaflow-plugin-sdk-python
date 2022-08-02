#!/bin/bash

set -e

protoc -I=proto --python_out=src/arcaflow_plugin_sdk proto/arcaflow-plugin.proto --experimental_allow_proto3_optional
