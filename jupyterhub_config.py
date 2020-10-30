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

c.JupyterHub.proxy_class = 'redirect-proxy'
c.JupyterHub.authenticator_class = 'dummyauthenticator.DummyAuthenticator'
c.JupyterHub.spawner_class = 'dataprocspawner.DataprocSpawner'
c.Spawner.project = "mam-nooage"

# Authenticator
# from gcpproxiesauthenticator.gcpproxiesauthenticator import GCPProxiesAuthenticator
# c.JupyterHub.authenticator_class = GCPProxiesAuthenticator
# c.GCPProxiesAuthenticator.check_header = "X-Inverting-Proxy-User-Id"
# c.GCPProxiesAuthenticator.template_to_render = "welcome.html"

# Must be 8080 to meet Inverting Proxy requirements.
c.JupyterHub.port = 8080

# Have JupyterHub listen on all interfaces
c.JupyterHub.hub_ip = '0.0.0.0'
# The IP address that other services should use to connect to the hub
c.JupyterHub.hub_connect_ip = socket.gethostbyname(socket.gethostname())

c.DataprocSpawner.dataproc_configs = "gs://ain-working/configs"
c.DataprocSpawner.dataproc_locations_list = "b,c"

c.Spawner.spawner_host_type = 'ain'

admins = os.environ.get('ADMINS', '')
if admins:
  c.Authenticator.admin_users = admins.split(',')

# Port can not be 8001. Conflicts with another one process.
c.ConfigurableHTTPProxy.api_url = 'http://127.0.0.1:8005'

# Option on Dataproc Notebook server to allow authentication.
c.Spawner.args = ['--NotebookApp.disable_check_xsrf=True']
