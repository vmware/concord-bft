#!/bin/bash

# Overly cautious saftey check: Make sure you're in the right directory
if [[ $PWD != *clientservice ]]; then
  >&2 echo "Error: Make sure you're in hermes/util/clientservice"
  exit 1;
fi

PROTOS=(
  $PWD/../../../client/proto/request/v1/request.proto
  $PWD/../../../client/proto/event/v1/event.proto
)

for proto in ${PROTOS[@]}; do
  python \
    -m grpc_tools.protoc \
    -I $(dirname ${proto}) \
    --python_out=. \
    --grpc_python_out=. \
    ${proto}
done
