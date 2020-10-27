#!/bin/bash

PROJECT_ID=$1
VM_NAME=$2
CONFIGS_LOCATION=$3
DOCKER_IMAGE="gcr.io/${PROJECT_ID}/dataprocspawner:ain"

cat <<EOT > Dockerfile
FROM jupyterhub/jupyterhub

# Install gcloud
RUN apt-get update && apt-get install -y curl apt-transport-https ca-certificates gnupg \
  && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
  && echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
  && apt-get update && apt-get install -y google-cloud-sdk

RUN apt-get update \
  && apt-get remove -y python3-pycurl \
  && apt-get install -y --no-install-recommends \
    libssl-dev \
    libcurl4-openssl-dev \
    python3-wheel \
    git \
    tcpdump \
  && apt-get purge \
  && apt-get clean -y

RUN pip install --upgrade pip \
  && pip install --upgrade \
    psycopg2-binary \
    google-api-python-client \
    google-auth-oauthlib \
    google-api-python-client \
    oauth2client \
    google-auth \
    googleapis-common-protos \
    google-auth-httplib2

RUN apt-get update && apt-get install -y libpq-dev \
    && apt-get install -y vim \
    && apt-get install -y iproute2 \
    && apt-get autoremove -y \
    && apt-get clean -y

RUN pip install jupyterhub-dummyauthenticator

COPY jupyterhub_config.py .

COPY . dataprocspawner/
RUN cd dataprocspawner && pip install .

RUN pip install git+https://github.com/GoogleCloudPlatform/jupyterhub-gcp-proxies-authenticator.git

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

import os
import requests
import socket

from tornado import web

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

c.Spawner.spawner_host_type = 'ain'

# Authenticator
from gcpproxiesauthenticator.gcpproxiesauthenticator import GCPProxiesAuthenticator
c.JupyterHub.authenticator_class = GCPProxiesAuthenticator
c.GCPProxiesAuthenticator.check_header = "X-Inverting-Proxy-User-Id"
c.GCPProxiesAuthenticator.template_to_render = "welcome.html"

admins = os.environ.get('ADMINS', '')
if admins:
  c.Authenticator.admin_users = admins.split(',')

# Port can not be 8001. Conflicts with another one process.
c.ConfigurableHTTPProxy.api_url = 'http://127.0.0.1:8005'

# Option on Dataproc Notebook server to allow authentication.
c.Spawner.args = ['--NotebookApp.disable_check_xsrf=True']

# Passes a Hub URL accessible by Dataproc. Without this AI Notebook passes a 
# local address. Used by the overwritter get_env().
metadata_base_url = "http://metadata.google.internal/computeMetadata/v1"
headers = {'Metadata-Flavor': 'Google'}
params = ( ('recursive', 'true'), ('alt', 'text') )
instance_ip = requests.get(
    f'{metadata_base_url}/instance/network-interfaces/0/ip', 
    params=params, 
    headers=headers
).text
c.Spawner.env_keep = ['NEW_JUPYTERHUB_API_URL']
c.Spawner.environment = {
  'NEW_JUPYTERHUB_API_URL': f'http://{instance_ip}:8080/hub/api'
}  

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
  --image="projects/deeplearning-platform-release/global/images/common-container-experimental-v20201014-debian-9" \
  --metadata="proxy-mode=service_account,container=${DOCKER_IMAGE},agent-health-check-path=/hub/health,jupyterhub-host-type=ain,framework=Dataproc Hub,agent-env-file=gs://dataproc-spawner-dist/env-agent,container-use-host-network=True"

# Clean up
rm Dockerfile
rm jupyterhub_config.py