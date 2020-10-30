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
""" Extends default objects.Server to modify server healthiness checks."""

import errno
import socket

from google.api_core import exceptions
from google.cloud.dataproc_v1beta2 import ClusterStatus

from jupyterhub.objects import Server
from jupyterhub.utils import exponential_backoff

from tornado import ioloop
from tornado.httpclient import HTTPError, AsyncHTTPClient
from tornado.log import app_log

class DataprocHubServer(Server):
  """ Extends the server class to wait up based on cluster status.

  By default, a notebook is considered to be healthy if its url returns any code
  < 500. The Component Gateway URL returns a 302 which would be healthy even if
  the notebook is not up and running yet. This class blocks the redirect until
  the Dataproc cluster is running or timeout is reached.
  """

  def __init__(self, dataproc_client, cluster_data, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self._dataproc_client = dataproc_client
    self._project = cluster_data['cluster_project']
    self._region = cluster_data['cluster_region']
    self._clustername = cluster_data['cluster_name']

  async def get_cluster_status(self):
    cluster = await self.get_cluster()
    if cluster is not None:
      return cluster.status.state
    return None

  async def get_cluster(self):
    app_log.debug(
      'Check cluster %s in region %s for project %s',
      self._clustername,
      self._region,
      self._project)
    try:
      return self._dataproc_client.get_cluster(
          project_id=self._project,
          region=self._region,
          cluster_name=self._clustername)
    except exceptions.NotFound:
      app_log.info('wait_for_dataprochub(): Cluster Not Found')
      return None

  def wait_up(
      self,
      timeout=100,
      http=False, # pylint: disable=unused-argument
      ssl_context=None # pylint: disable=unused-argument
  ):
    """ Waits for this server to come up. """
    return self.wait_for_dataprochub(timeout=timeout)

  async def wait_for_dataprochub(self, timeout=10):
    """ Waits for a status of RUNNING """
    loop = ioloop.IOLoop.current()
    tic = loop.time() # pylint: disable=unused-variable
    client = AsyncHTTPClient()

    async def is_reachable():
      """ Checks if notebooks is ready to be used.

      self.base_url and self.ip are values that were given to the DataprocHubServer.
      But they differ from DB. Redirect seems to care only about the DB due to
      user.py > spawner.

      Component Gateway URL should be up and running but redirects. JupyterHub
      sees that as a results. Blocks the HTTPResponse until the cluster is
      up and running.

      user.wait_up expects an HTTPResponse
      """
      unfetchable = 'http://1.2.3.4:443'
      try:
        status = await self.get_cluster_status()

        if status == ClusterStatus.State.ERROR:
          # TODO(mayran): Write a better error.
          raise RuntimeError('There was an error when starting the cluster.')

        if status is None or status != ClusterStatus.State.RUNNING:
          r_failure = await client.fetch(unfetchable, follow_redirects=False)
          return r_failure

        app_log.info('Returns r_success %s with url %s', status, self.url)
        r_success = await client.fetch(self.url, follow_redirects=True)
        return r_success

      except (exceptions.NotFound, exceptions.PermissionDenied) as e:
        app_log.info('is_reachable(): Dataproc exception %s', e.message)

      except HTTPError as e:
        if e.code >= 500:
          if e.code != 599:
            # Expects 599 for no connection but might get some others.
            app_log.into(
                'Server at %s responded with error: %s', self.url, e.code)
        else:
          app_log.info('Server at %s returned error %s', self.url, e.code)
          return e.response

      except (OSError, socket.error) as e:
        if e.errno not in {errno.ECONNABORTED, errno.ECONNREFUSED, errno.ECONNRESET}:
          app_log.warning('Failed to connect to %s', self.url)

      return False

    resp = await exponential_backoff(
        is_reachable,
        f'Server did not respond in {timeout} seconds',
        timeout=timeout,
    )
    return resp
