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
c.Spawner.project = "mam-nooage"

# Must be 8080 to meet Inverting Proxy requirements.
c.JupyterHub.port = 8080

import socket

# Have JupyterHub listen on all interfaces
c.JupyterHub.hub_ip = '0.0.0.0'
# The IP address that other services should use to connect to the hub
c.JupyterHub.hub_connect_ip = socket.gethostbyname(socket.gethostname())

c.DataprocSpawner.dataproc_configs = "gs://ain-working/configs"
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
