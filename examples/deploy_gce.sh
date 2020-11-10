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
#
echo "Usage: ./examples/deploy_gce.sh PROJECT_ID CONFIGS_LOCATION VM_NAME."

PROJECT_ID=$1
CONFIGS_LOCATION=$2
VM_NAME=$3
DOCKER_IMAGE="gcr.io/${PROJECT_ID}/dataprocspawner:gce"

# gcloud builds submit does not support -f like docker build.
cat <<EOT > ./examples/cloudbuild.yaml
steps:
- name: "gcr.io/cloud-builders/docker"
  args:
  - build
  - "--tag=${DOCKER_IMAGE}"
  - "--file=./examples/docker/Dockerfile.example"
  - .
images:
- ${DOCKER_IMAGE}
EOT

#gcloud --project "${PROJECT_ID}" builds submit -t "${DOCKER_IMAGE}" -f .
gcloud --project "${PROJECT_ID}" builds submit --config=./examples/cloudbuild.yaml .

gcloud beta compute instances create-with-container "${VM_NAME}" \
  --project "${PROJECT_ID}" \
  --container-image="${DOCKER_IMAGE}" \
  --container-arg="--DataprocSpawner.project=${PROJECT_ID}" \
  --container-arg="--DataprocSpawner.dataproc_configs=${CONFIGS_LOCATION}" \
  --scopes=cloud-platform \
  --zone us-central1-a

gcloud compute instances describe "${VM_NAME}" \
  --project "${PROJECT_ID}" \
  --zone us-central1-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'

rm ./examples/cloudbuild.yaml