#!/bin/bash

PROJECT_ID=$1
VM_NAME=$2
CONFIGS_LOCATION=$3
DOCKER_IMAGE="gcr.io/${PROJECT_ID}/dataprocspawner:ain"

cat <<EOT > Dockerfile
FROM jupyterhub/jupyterhub

RUN pip install jupyterhub-dummyauthenticator

COPY jupyterhub_config.py .

COPY . dataprocspawner/
RUN cd dataprocspawner && pip install .

COPY templates /etc/jupyterhub/templates

EXPOSE 8080

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
c.Spawner.project = "${PROJECT_ID}"

# Must be 8080 to meet Inverting Proxy requirements.
c.JupyterHub.port = 8080

import socket

# Have JupyterHub listen on all interfaces
c.JupyterHub.hub_ip = '0.0.0.0'
# The IP address that other services should use to connect to the hub
c.JupyterHub.hub_connect_ip = socket.gethostbyname(socket.gethostname())

c.DataprocSpawner.dataproc_configs = "${CONFIGS_LOCATION}"
c.DataprocSpawner.dataproc_locations_list = "b,c"

# TODO(mayran): Move the handler into Python code
# and properly log Component Gateway being None.
from jupyterhub.handlers.base import BaseHandler
from tornado.web import authenticated

class RedirectComponentGatewayHandler(BaseHandler):
  @authenticated
  async def get(self, user_name='', user_path=''):
    next_url = self.current_user.spawner.component_gateway_url
    if next_url:
      self.redirect(next_url)
    self.redirect('/404')
    
c.JupyterHub.extra_handlers = [
  (r"/redirect-component-gateway(/*)", RedirectComponentGatewayHandler),
]
c.JupyterHub.template_paths = ['/etc/jupyterhub/templates']
EOT

gcloud builds submit -t "${DOCKER_IMAGE}" .

gcloud beta compute instances create "${VM_NAME}" \
  --project "${PROJECT_ID}" \
  --scopes=cloud-platform \
  --zone us-central1-a \
  --image="projects/deeplearning-platform-release/global/images/common-container-experimental-v20200912-debian-9" \
  --metadata="proxy-mode=service_account,container=${DOCKER_IMAGE},agent-health-check-path=/hub/health,jupyterhub-host-type=ain,framework=Dataproc Hub,agent-env-file=gs://dataproc-spawner-dist/env-agent,container-use-host-network=True"