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

echo "Usage: ./deploy_local_example.sh PROJECT_ID CONFIGS_LOCATION USER_EMAIL."

PROJECT=$1
CONFIGS_LOCATION=$2
USER_EMAIL=$3
PORT="${4:-8000}"

cat <<EOT > Dockerfile
FROM jupyterhub/jupyterhub

RUN pip install jupyterhub-dummyauthenticator

RUN apt-get update \
  && apt-get remove -y vim

COPY jupyterhub_config.py .

COPY . dataprocspawner/
RUN cd dataprocspawner && pip install .

COPY dataprochub dataprochub

COPY templates /etc/jupyterhub/templates

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

c.DataprocSpawner.dataproc_configs = "${CONFIGS_LOCATION}"
c.DataprocSpawner.dataproc_locations_list = "b,c"

c.JupyterHub.log_level = 'DEBUG'

# TODO(mayran): Move the handler into Python code
# and properly log Component Gateway being None.
# from jupyterhub.handlers.base import BaseHandler
# from tornado.web import authenticated

# class RedirectComponentGatewayHandler(BaseHandler):
#   """ Handles redirect to notebook server after spawn.

#   This only works when the spawn_pending page is displayed.
#   For the next access, use the db that is updated by _handle_operation_done.
#   """
#   @authenticated
#   async def get(self, user_name='', user_path=''):
#     next_url = self.current_user.spawner.component_gateway_url
#     if next_url:
#       self.redirect(next_url)
#     self.redirect('/404')

# c.JupyterHub.extra_handlers = [
#   (r"/redirect-component-gateway(/*)", RedirectComponentGatewayHandler),
# ]
c.JupyterHub.template_paths = ['/etc/jupyterhub/templates']
EOT

mkdir -p /tmp/keys
# cp ~/.config/gcloud/application_default_credentials.json /tmp/keys
cp ~/.config/gcloud/legacy_credentials/"${USER_EMAIL}"/adc.json /tmp/keys/application_default_credentials.json

GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/application_default_credentials.json

docker build -t ain .

# Clean up
rm Dockerfile
rm jupyterhub_config.py

docker run -it \
-p "${PORT}":8000 \
-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/application_default_credentials.json  \
-v "$GOOGLE_APPLICATION_CREDENTIALS":/tmp/keys/application_default_credentials.json:ro \
-e GOOGLE_CLOUD_PROJECT="${PROJECT}" \
ain:latest \
--DataprocSpawner.project="${PROJECT}"