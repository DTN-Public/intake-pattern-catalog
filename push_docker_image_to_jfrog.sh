#! /bin/bash

# This builds a docker image for use in bitbucket-pipelines, and pushes it to
# cloud artifactory
# If the environment variable bamboo_REPO isn't set, it default to pushing to the
# dev repo. It puts the newly-created image tag to common/dags/SIA_IMAGE

#REPO="dtn-cr-dev.jfrog.io/dtn/dsci/pipelines"
REPO="dtn-docker-dev-local.jfrog.io/dtn/dsci/pipelines"

echo "REPO=$REPO"

IMAGE="$REPO:latest"

docker image build -t "$IMAGE" -f Dockerfile .

docker push "$IMAGE"