#!/bin/bash
#
# Usage: bash deploy_gce_example.sh <PROJECT_ID> <VM_NAME>

PROJECT_ID=$1
VM_NAME=$2
DOCKER_IMAGE="gcr.io/${PROJECT_ID}/dataprocspawner:gce"


cat <<EOT > Dockerfile
FROM jupyterhub/jupyterhub

RUN pip install jupyterhub-dummyauthenticator

COPY jupyterhub_config.py .

COPY . dataprocspawner/
RUN cd dataprocspawner && pip install .

ENTRYPOINT ["jupyterhub"]
EOT

cat <<EOT > jupyterhub_config.py
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

c.JupyterHub.authenticator_class = 'dummyauthenticator.DummyAuthenticator'
c.JupyterHub.spawner_class = 'dataprocspawner.DataprocSpawner'
# The port that the spawned notebook listens on for the hub to connect
c.Spawner.port = 12345

import socket

# Have JupyterHub listen on all interfaces
c.JupyterHub.hub_ip = '0.0.0.0'
# The IP address that other services should use to connect to the hub
c.JupyterHub.hub_connect_ip = socket.gethostbyname(socket.gethostname())
EOT


gcloud builds submit -t  ${DOCKER_IMAGE} .

gcloud beta compute instances create-with-container ${VM_NAME} \
  --project ${PROJECT_ID} \
  --container-image=${DOCKER_IMAGE} \
  --container-arg="--DataprocSpawner.project=${PROJECT_ID}" \
  --scopes=cloud-platform \
  --zone us-central1-a

gcloud compute instances describe ${VM_NAME} \
  --project ${PROJECT_ID} \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'

# Clean up
rm Dockerfile
rm jupyterhub_config.py
