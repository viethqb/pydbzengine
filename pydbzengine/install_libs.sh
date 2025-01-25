#!/usr/bin/env bash

CURRENT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

find "${CURRENT_DIR}/debezium/libs" -type f -delete
mvn dependency:copy-dependencies -DoutputDirectory="${CURRENT_DIR}/debezium/libs"