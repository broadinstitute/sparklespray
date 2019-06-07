#!/bin/bash

set -ex

docker build . -t pmontgom/sparkles-consume-setup:v1
docker push pmontgom/sparkles-consume-setup:v1

