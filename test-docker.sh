#!/bin/bash
set -e
bash build-docker.sh


docker run \
-it \
--rm \
--name=openagents-document-retrieval \
openagents-document-retrieval