#!/bin/bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "Usage: ./examples/deploy_local.sh PROJECT_ID CONFIGS_LOCATION USER_EMAIL."

PROJECT=$1
CONFIGS_LOCATION=$2
USER_EMAIL=$3
PORT="${4:-8000}"
DOCKER_IMAGE="hub:local"

# Manages authentication for container
mkdir -p /tmp/keys
# cp ~/.config/gcloud/application_default_credentials.json /tmp/keys
cp ~/.config/gcloud/legacy_credentials/"${USER_EMAIL}"/adc.json /tmp/keys/application_default_credentials.json
GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/application_default_credentials.json

# Builds
docker build -t "${DOCKER_IMAGE}" -f docker/Dockerfile .

# Runs
# For named servers, add -e HUB_ALLOW_NAMED_SERVERS="true"
docker run -it \
-p "${PORT}":8080 \
-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/application_default_credentials.json  \
-v "$GOOGLE_APPLICATION_CREDENTIALS":/tmp/keys/application_default_credentials.json:ro \
-e GOOGLE_CLOUD_PROJECT="${PROJECT}" \
-e PROJECT="${PROJECT}" \
-e DATAPROC_CONFIGS="${CONFIGS_LOCATION}" \
-e JUPYTERHUB_REGION="us-west1" \
-e DATAPROC_ALLOW_CUSTOM_CLUSTERS=true \
-e FORCE_SINGLE_USER=true \
-e HUB_ALLOW_NAMED_SERVERS="true" \
-e ALLOW_RANDOM_CLUSTER_NAMES="true" \
-e DUMMY_EMAIL="${USER_EMAIL}" \
"${DOCKER_IMAGE}"