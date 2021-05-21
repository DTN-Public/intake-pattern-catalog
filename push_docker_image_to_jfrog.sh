#! /bin/bash

# This builds a docker image for use in bitbucket-pipelines, and pushes it to
# cloud artifactory. The first argument is the tag

REPO="dtn-cr-dev.jfrog.io/dtn/dsci/pipelines"

echo "REPO=$REPO"

IMAGE="$REPO:$1"

docker image build -t "$IMAGE" -f Dockerfile .

docker push "$IMAGE"