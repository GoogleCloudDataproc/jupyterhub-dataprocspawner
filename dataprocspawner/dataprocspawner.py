# Copyright 2019 Google LLC
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

"""Spawner that can start/stop Dataproc clusters and is intended to be used with JupyterHub.

Spawner that will spin up a Dataproc cluster with the Jupyter component enabled
but run Jupyterhub-singleuser instead of Jupyter notebook.
Jupyterhub-singleuser is just a Jupyter notebook that is capable of dialing
back into Jupyterhub.

The spawner passes environment variables into dataproc:jupyter.hub.env as a
JSON-parsable string that will be parsed in the Jupyter optional component.
The spawner passes arguments into dataproc:jupyter.hub.args as a space-separated
string.
"""

import json
from jupyterhub.spawner import Spawner
from google.api_core import exceptions
from google.cloud import dataproc_v1beta2
from google.cloud.dataproc_v1beta2.gapic.enums import ClusterStatus
from google.cloud.dataproc_v1beta2.gapic.transports import (
    cluster_controller_grpc_transport)
from traitlets import List, Unicode

class DataprocSpawner(Spawner):

  """Spawner for Dataproc clusters.

  Reference: https://jupyterhub.readthedocs.io/en/stable/reference/spawners.html
  """

  poll_interval = 5

  # Since creating a cluster takes longer than the 30 second default,
  # up this value so Jupyterhub can connect to the spawned server.
  # Unit is in seconds.
  http_timeout = 900

  project = Unicode(
            '',
            help="""
            The project on Google Cloud Platform that the Dataproc clusters
            should be created under.

            This must be configured.
            """,
          ).tag(config=True)

  region = Unicode(
            'us-central1',
            help="""
            The region in which to run the Dataproc cluster.

            Defaults to us-central1. Currently does not support using
            'global' because the initialization for the cluster gRPC
            transport would be different.
            """,
          ).tag(config=True)

  zone = Unicode(
            'us-central1-a',
            help="""
            The zone in which to run the Dataproc cluster.

            Defaults to us-central1-a.
            """,
          ).tag(config=True)

  # Overwrite the env_keep from Spawner to only include PATH and LANG
  env_keep = List(
          [
            'PATH',
            'LANG',
          ],
          help="""
          Whitelist of environment variables for the single-user server to inherit from the JupyterHub process.
          This whitelist is used to ensure that sensitive information in the JupyterHub process's environment
          (such as `CONFIGPROXY_AUTH_TOKEN`) is not passed to the single-user server's process.
          """,
        ).tag(config=True)


  def __init__(self, *args, **kwargs):
    _mock = kwargs.pop('_mock', False)
    super().__init__(*args, **kwargs)

    if _mock:
      # Mock the API
      self.dataproc = kwargs.get('dataproc')
    else:
      self.client_transport = (
        cluster_controller_grpc_transport.ClusterControllerGrpcTransport(
            address=f'{self.region}-dataproc.googleapis.com:443'))
      self.dataproc = dataproc_v1beta2.ClusterControllerClient(
          self.client_transport)

  def getDataprocMasterFQDN(self):
    # Zonal DNS is in the form
    # [CLUSTER NAME]-m.[ZONE].c.[PROJECT ID].internal
    # If the project is domain-scoped, then PROJECT ID needs to be in the form
    # [PROJECT NAME].[DOMAIN]
    # More info here: https://cloud.google.com/compute/docs/internal-dns#instance-fully-qualified-domain-names

    if ':' in self.project:
      # Domain-scoped project
      # project_split[0] is the domain name
      project_split = self.project.split(':')
      return f'{self.clustername()}-m.{self.zone}.c.{project_split[1]}.{project_split[0]}.internal'
    else:
      return f'{self.clustername()}-m.{self.zone}.c.{self.project}.internal'

  async def start(self):

    if await self.exists(self.clustername()):
      self.log.warning(f'Cluster named {self.clustername()} already exists')
    else:
      await self.create_cluster()

      start_notebook_cmd = self.cmd + self.get_args()
      start_notebook_cmd = ' '.join(start_notebook_cmd)
      self.log.info(start_notebook_cmd)

    # Return the FQDN of the spawned cluster
    return (self.getDataprocMasterFQDN(), self.port)

  async def create_cluster(self):
    self.log.info(f'Creating cluster with name {self.clustername()}')

    # Dump the environment variables, including those generated after init
    # (ex. JUPYTERHUB_API_TOKEN)
    # Manually set PATH for testing purposes
    self.temp_env = self.get_env()
    self.temp_env["PATH"] = "/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    self.env_str = json.dumps(self.temp_env)
    self.args_str = ' '.join(self.get_args())

    self.zone_uri = f'https://www.googleapis.com/compute/v1/projects/{self.project}/zones/{self.zone}'

    #TODO(annyue): make the fields in cluster_data traitlets
    self.cluster_data = {
        'cluster_name': self.clustername(),
        'config': {
            'gce_cluster_config': {
                'zone_uri': self.zone_uri
            },
            'software_config': {
                'image_version': '1.4-debian9',
                'properties': {
                    'dataproc:jupyter.hub.enabled': 'true',
                    'dataproc:jupyter.hub.args': self.args_str,
                    'dataproc:jupyter.hub.env': self.env_str
                },
                'optional_components': [
                    'ANACONDA',
                    'JUPYTER'
                ]
            }
        }
    }

    cluster = self.dataproc.create_cluster(self.project, self.region,
                                           self.cluster_data)
    return cluster

  async def stop(self):
    self.log.info(f'Stopping cluster with name {self.clustername()}')
    if await self.exists(self.clustername()):
      result = self.dataproc.delete_cluster(
          project_id=self.project,
          region=self.region,
          cluster_name=self.clustername())
      return result

  async def poll(self):
    status = await self.get_cluster_status(self.clustername())
    if status is None or status in (ClusterStatus.State.ERROR, ClusterStatus.State.DELETING, ClusterStatus.State.UNKNOWN):
      return 1
    elif status == ClusterStatus.State.CREATING:
      self.log.info(f'{self.clustername()} is creating')
      return None
    elif status in (ClusterStatus.State.RUNNING, ClusterStatus.State.UPDATING):
      self.log.info(f'{self.clustername()} is up and running')
      return None

  async def get_cluster_status(self, clustername):
    cluster = await self.get_cluster(self.clustername())
    if cluster is not None:
      return cluster.status.state
    else:
      return None

  async def exists(self, clustername):
    return (await self.get_cluster(clustername)) is not None

  async def get_cluster(self, clustername):
    try:
      return self.dataproc.get_cluster(self.project, self.region, self.clustername())
    except exceptions.NotFound:
      return None

  # JupyterHub provides a notebook per user, so the username is
  # used to distinguish between clusters.
  def clustername(self):
    return f'{self.user.name}-jupyterhub'
