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

""" Extends default Proxy to limit which routes are added to the proxy. """

import re
from jupyterhub.proxy import ConfigurableHTTPProxy

class RedirectProxy(ConfigurableHTTPProxy):

  def __init__(self, **kwargs):
    self.log.info(f'# Initializing RedirectProxy.')
    super().__init__(**kwargs)

  async def add_route(self, routespec, target, data):
    """ Adds route only if target is not a Component Gateway URL.

    Using the default add_route would return a 404 when accessing /user/<USER>.
    """
    reg = r'(https:\/\/)*[a-zA-Z0-9]*-dot-[a-z1-9-]*\.dataproc\.googleusercontent\.com'
    if re.match(reg, target):
      self.log.debug(f'# Skip adding {target} to the routes.')
      return
    await super().add_route(routespec, target, data)
