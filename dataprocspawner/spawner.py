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
"""A custom Spawner that creates notebooks backed by Dataproc clusters."""
import asyncio
import json
import math
import os
import random
import re
import string
from datetime import datetime as dt
from types import SimpleNamespace

import proto
import requests
import yaml
from async_generator import aclosing, async_generator, yield_
from dataprocspawner.customize_cluster import (
    get_base_cluster_html_form,
    get_custom_cluster_html_form,
)
from dataprocspawner.spawnable import DataprocHubServer
from google.api_core import exceptions
from google.cloud import logging_v2, storage
from google.cloud.dataproc_v1beta2 import (
    Cluster,
    ClusterControllerClient,
    ClusterStatus,
)
from google.cloud.dataproc_v1beta2.services.cluster_controller.transports import (
    ClusterControllerGrpcTransport,
)
from google.cloud.dataproc_v1beta2.types.shared import Component
from google.protobuf.json_format import MessageToDict
from googleapiclient import discovery
from jupyterhub import orm
from jupyterhub.spawner import Spawner
from tornado import web
from traitlets import Bool, Dict, List, Unicode


def url_path_join(*pieces):
  """Join components of url into a relative url.

  Use to prevent double slash when joining subpath. This will leave the
  initial and final / in place.

  Copied from `notebook.utils.url_path_join`.
  """
  initial = pieces[0].startswith('/')
  final = pieces[-1].endswith('/')
  stripped = [s.strip('/') for s in pieces]
  result = '/'.join(s for s in stripped if s)
  if initial:
    result = '/' + result
  if final:
    result = result + '/'
  if result == '//':
    result = '/'
  return result

def _validate_proto(data, proto_cls):
  """Utility method to strip unknown fields and enum values for proto parsing.

  This method recursively calls _validate_proto_field for each field of the
  proto, deleting unknown or invalid fields as it goes.

  Args:
  - dict data: representation of a proto as a (possibly nested) dict
  - proto.message.MessageMeta proto_cls: class of the proto to which the data
                                         should be parsed
  Returns:
    [String]: List of warnings of fields removed
  """
  if not isinstance(proto_cls, proto.message.MessageMeta):
    return
  if not data or not isinstance(data, dict):
    return

  meta = proto_cls._meta # pylint: disable=protected-access
  warnings = []
  # Iterate over copy of data to avoid concurrent iteration and modification
  for field in data.copy():
    if field in meta.fields:
      field_valid, new_warnings = _validate_proto_field(data[field], meta.fields[field])
      warnings.extend(new_warnings)
      if not field_valid:
        warnings.append(f'Removing unknown/bad value {data[field]} for field {field}.')
        del data[field]

    else:
      warnings.append(f'Removing unknown field {field} for class {proto_cls}')
      del data[field]

  return warnings

def _validate_proto_field(data, field_descriptor):
  """Helper function for validating a single field of a proto.

  *Only* validates message and enum fields.
  Recursively calls _validate_proto for message fields.

  Args:
    - Any data: representation of a single proto field. This could be
                any valid type.
    - proto.fields.Field field_descriptor: Field descriptor for a single
                                           proto field.
  Returns:
    (Bool is_valid, [String] warnings):
      is_valid: True if field is valid after this method returns. False if
                field is still invalid after this method returns. If False,
                the caller should either fail or remove offending field.
      warnings: List of warnings of fields removed."""
  if not data:
    return True, []
  fd = field_descriptor
  warnings = []

  if fd.message and isinstance(fd.message, proto.message.MessageMeta):
    # Don't validate map fields
    if fd.message._meta.options and fd.message._meta.options.map_entry: # pylint: disable=protected-access
      return True, warnings

    to_validate = [data] if not fd.repeated else data
    for entry in to_validate:
      warnings.extend(_validate_proto(entry, fd.message))
    return True, warnings

  elif fd.enum:
    if fd.repeated:
      # For repeated enum fields, filter out unknown values ourselves
      to_del = []
      for i, val in enumerate(data):
        if val not in fd.enum.__members__:
          warnings.append(f'Removing unknown/bad value {val} for field {fd.name}.')
          to_del.append(i)
      # Delete from the back so we don't mess up later indices
      to_del.reverse()
      for i in to_del:
        del data[i]
      return True, warnings

    else:
      # For non-repeated enum fields, let the caller strip the invalid value
      return (data in fd.enum.__members__, warnings)

  # Other types we don't attempt to validate
  return True, warnings

class DataprocSpawner(Spawner):
  """Spawner for Dataproc clusters.

  Reference: https://jupyterhub.readthedocs.io/en/stable/reference/spawners.html
  """

  poll_interval = 5

  # Since creating a cluster takes longer than the 30 second default,
  # up this value so Jupyterhub can connect to the spawned server.
  # Unit is in seconds.
  http_timeout = 1800

################################################################################
# Admin variables passed in jupytherhub_config.py as c.Spawner.[VARIABLE]
################################################################################
  project = Unicode(
      config=True,
      help="""
      The project on Google Cloud Platform that the Dataproc clusters
      should be created under.

      This must be configured.
      """,)

  region = Unicode(
      'us-central1',
      config=True,
      help="""
      The region in which to run the Dataproc cluster.
      Defaults to us-central1. Currently does not support using
      'global' because the initialization for the cluster gRPC
      transport would be different.
      """,)

  zone = Unicode(
    'us-central1-a',
      config=True,
      help=""" The zone in which to run the Dataproc cluster.""",)

  cluster_data = Dict(
      config=True,
      help="""
      Admin provided dict for setting up Dataproc cluster. If this field is not
      provided, the cluster configuration is set using YAML files on GCE. """,)

  gcs_notebooks = Unicode(
      config=True,
      help="""
      GCS location to save Notebooks for a stateful experience.

      This must be configured.
      """,)

  gcs_user_folder = Unicode(
      config=True,
      help=""" GCS location to save the user's Notebooks. """,)

  dataproc_configs = Unicode(
      config=True,
      help="""
      Comma separated list of the dataproc configurations available in the
      user spawning form. Each path can be a bucket, subfolder or file and can
      include the prefix gs:// or not and the suffix / or not.

      Example: 'bucket/configs/,gs://bucket/configs/file.yaml,gs://bucket'
      """,)

  dataproc_default_subnet = Unicode(
      config=True,
      help="""
      GCP subnet where to deploy the spawned Cloud Dataproc cluster. If not
      provided in the config yaml, defaults to the same as JupyterHub.
      """,)

  dataproc_service_account = Unicode(
      config=True,
      help="""
      This solution uses a default service account for all spawned cluster if
      not provided by the administrator.
      """,)

  dataproc_locations_list = Unicode(
      '',
      config=True,
      help="""
      Comma separated list of the zone letters where to spawn Cloud Dataproc in
      the JupyterHub region.
      Example: 'a,b'

      This must be configured.
      """,)

  idle_checker = Dict(
      {'idle_job_path': '', 'idle_path': '', 'timeout': '60m'},
      config=True,
      help="""
          Set up shutdown of a cluster after some idle time.
          Base on https://github.com/blakedubois/dataproc-idle-check
          idle_job - gcs path to https://github.com/blakedubois/dataproc-idle-check/blob/master/isIdleJob.sh
          idle_path - gcs path to https://github.com/blakedubois/dataproc-idle-check/blob/master/isIdle.sh
          timeout - idle time after which cluster will be shutdown
          Check official documentation: https://github.com/blakedubois/dataproc-idle-check
          """,)

  allow_custom_clusters = Bool(
      False,
      config=True,
      help=""" Allow users to customize their cluster. """,)

  allow_random_cluster_names = Bool(
      False,
      config=True,
      help=""" Allow users to randomize their cluster names. """,)

  default_notebooks_gcs_path = Unicode(
      '',
      config=True,
      help="""
      The gcs path where default notebooks stored. Don't load default
      notebooks if variable is empty.
      """,)

  default_notebooks_folder = Unicode(
      'examples/',
      config=True,
      help='The name of folder into which service will copy default notebooks',)

  machine_types_list = Unicode(
      '',
      config=True,
      help='Allowed machine types',)

  # Overwrites the env_keep from Spawner to only include PATH and LANG
  env_keep = List(
      ['PATH', 'LANG'],
      config=True,
      help="""
      Whitelist of environment variables for the single-user server to inherit
      from the JupyterHub process. This whitelist ensures that sensitive
      information in the JupyterHub process's environment (such as
      `CONFIGPROXY_AUTH_TOKEN`) is not passed to the single-user server's
      process.
      """,)

  spawner_host_type = Unicode(
      '',
      config=True,
      help='Host type on which the Spawner is running (e.g. gce, ain)',)

  force_add_jupyter_component = Bool(
      True,
      config=True,
      help="""
      Whether to always enable the JUPYTER and ANACONDA optional components
      even if not explicitly specified in the cluster config. It is recommended
      to set this to True, as clusters without these components will *not* function
      correctly when spawned.
      """,)

  cluster_name_pattern = Unicode(
      'dataprochub-{}',
      config=True,
      help="""
      Format string for name of the Dataproc cluster spawned for each user.
      The cluster name will be generated by calling cluster_name_pattern.format(username).
      """)

  show_spawned_clusters_in_notebooks_list = Bool(
      False,
      config=True,
      help="""
      Whether to show spawned single-user clusters in the list of Dataproc Notebooks.
      """)

  force_single_user = Bool(
    False,
    config=True,
    help="""Whether a notebook on a cluster can only be accessed by the user who
    spawned it.
    """)

  def __init__(self, *args, **kwargs):
    mock = kwargs.pop('_mock', False)
    super().__init__(*args, **kwargs)

    # https://googleapis.dev/python/google-api-core/latest/operation.html
    self.operation = None
    self.component_gateway_url = None
    self.progressor = {}

    metadata_instance = 'http://metadata.google.internal/computeMetadata/v1/instance'

    # Defaults region if not specified to Dataproc Hub's region.
    if not self.region:
      try:
        r = requests.get(
            f'{metadata_instance}/zone',
            headers={'Metadata-Flavor': 'Google'})
        r.raise_for_status()
        self.region = '-'.join(r.text.split('/')[-1].split('-')[:-1])
        self.log.info(f'# Using region {self.region}')
      except Exception as e:  ## pylint: disable=broad-except
        self.log.info(
          'Fetching Hub region failed. Probably not running on GCE. '
          f'Consider setting JUPYTERHUB_REGION as a container env: {e}.')

    # Check if we have a notebooks proxy URL
    self.hub_host = ''
    if 'hub_host' in kwargs:
      self.hub_host = kwargs.get('hub_host')
    else:
      try:
        r = requests.get(
            f'{metadata_instance}/attributes/proxy-url',
            headers={'Metadata-Flavor': 'Google'})
        r.raise_for_status()
        self.hub_host = f'https://{r.text}/'
        self.log.info(f'Got proxy url {r.text} from metadata')
      except Exception as e:  ## pylint: disable=broad-except
        self.log.info(f'Failed to get proxy url from metadata: {e}')

    if mock:
      # Mock the API
      self.dataproc_client = kwargs.get('dataproc')
      self.gcs_client = kwargs.get('gcs')
      self.logging_client = kwargs.get('logging')
      self.compute_client = kwargs.get('compute')
    else:
      self.client_transport = (
          ClusterControllerGrpcTransport(
              host=f'{self.region}-dataproc.googleapis.com:443'))
      self.dataproc_client = ClusterControllerClient(
          client_options={'api_endpoint':
                          f'{self.region}-dataproc.googleapis.com:443'})
      self.gcs_client = storage.Client(project=self.project)
      self.logging_client = logging_v2.LoggingServiceV2Client()
      self.compute_client = discovery.build('compute', 'v1', cache_discovery=False)

    if self.gcs_notebooks:
      if self.gcs_notebooks.startswith('gs://'):
        self.gcs_notebooks = self.gcs_notebooks[5:]

      self.gcs_user_folder = f'gs://{self.gcs_notebooks}/{self.get_username()}'

    if self.allow_random_cluster_names:
      self.rand_str = '-' + self._get_rand_string(4)
    else:
      self.rand_str = ''

    # Defaults zones if not specified or validate given zones if specified.
    self.dataproc_zones = self._validate_zones(self.region, self.dataproc_locations_list)

    self.progressor[self.clustername()] = SimpleNamespace(
        bar=0, logging=set(), start='', yields=[])


  ##############################################################################
  # Required functions
  ##############################################################################
  async def start(self):
    """ Creates a Dataproc cluster.
    If a cluster with the same name already exists, logs a warning and returns
    the same values as if creating the cluster.

    Returns:
      (String, Int): FQDN of the master node and the port it's accessible at.
    """
    if not self.project:
      self._raise_exception('You need to set a project')

    if (await self.get_cluster_status(self.clustername())
        == ClusterStatus.State.DELETING):
      self._raise_exception(f'Cluster {self.clustername()} is pending deletion.')

    if not await self.exists(self.clustername()):
      self.create_example_notebooks()
      self.operation = await self.create_cluster()

    self.component_gateway_url = await self.get_cluster_notebook_endpoint(
        self.clustername())

    start_notebook_cmd = self.cmd + self.get_args()
    start_notebook_cmd = ' '.join(start_notebook_cmd)
    self.log.info(f'start_notebook_cmd is: {start_notebook_cmd}')

    # Server needs default values even if user.py>spawn uses dynamic ones.
    # base_url gets overwritten.
    orm_server = orm.Server(
        proto='https',
        ip=self.component_gateway_url,
        port=443,
        base_url=self.user.base_url,
        cookie_name='cookie')
    cluster_data = {
      'cluster_name': self.clustername(),
      'cluster_project': self.project,
      'cluster_region': self.region,
    }
    self._server = DataprocHubServer(
        dataproc_client=self.dataproc_client,
        cluster_data=cluster_data,
        connect_url=self.component_gateway_url, # Forces self.url to return this
        orm_server=orm_server)

    return self.component_gateway_url

  async def stop(self):
    """ Stops an existing cluster """
    self.log.info(f'Stopping cluster with name {self.clustername()}')
    if await self.exists(self.clustername()):
      result = self.dataproc_client.delete_cluster(
          project_id=self.project,
          region=self.region,
          cluster_name=self.clustername())
      return result
    self.log.info(f'No cluster with name {self.clustername()}')
    return None

  async def poll(self):
    status = await self.get_cluster_status(self.clustername())
    if status is None or status in (ClusterStatus.State.DELETING,
                                    ClusterStatus.State.UNKNOWN):
      return 1
    elif status == ClusterStatus.State.ERROR:
      return 1
    elif status == ClusterStatus.State.CREATING:
      self.log.info(f'{self.clustername()} is creating.')
      return None
    elif status in (ClusterStatus.State.RUNNING, ClusterStatus.State.UPDATING):
      self.log.info(f'{self.clustername()} is up and running.')
      return None

  @async_generator
  async def _generate_progress(self):
    """Private wrapper of progress generator

    This method is always an async generator and will always yield at least one
    event. Note that adding `ready: True` is ignored as it is set directly in
    users.py based on spawner.ready.
    """
    if not self._spawn_pending or not self.operation:
      self.log.warning(
          "Spawn not pending, can't generate progress for %s", self._log_name)
      return

    operation_id = self.operation.operation.name.split('/')[-1]
    cluster_uuid = self.operation.metadata.cluster_uuid
    message_uuid = f'Operation {operation_id} for cluster uuid {cluster_uuid}'

    # Recovers existing progressor status is not new or on the initial step
    # named yields_start.
    spawner_progressor = self.progressor[self.clustername()]
    self.log.debug(f'# Running _generate_progress with {spawner_progressor}.')
    if len(spawner_progressor.yields) == 0:
      yields_start = (
        {'progress': 0, 'message': 'Server requested.'},
        {'progress': 5, 'message': message_uuid},
      )
      spawner_progressor.bar = yields_start[-1]['progress']
      spawner_progressor.logging.add('yields_start')
      spawner_progressor.yields += list(yields_start)
      spawner_progressor.start = dt.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    for y in spawner_progressor.yields:
      await yield_(y)

    async with aclosing(self.progress()) as progress:
      async for event in progress:
        await yield_(event)

  @async_generator
  async def progress(self):
    """ Loads bars progressively and displays cluster logs.

    Uses Cloud Logging method names to define a list of logs to display on the
    progress page. Those methods frames how many steps the progress bar takes
    before the cluster creation operation is done.

    If the operation is succesful, the page redirects to the notebook as usual
    with JupyterHub.

    If the operation failes, the progress bar is red and the user remains on the
    page. Error logs are displayed on the page.
    """
    spawner_progressor = self.progressor[self.clustername()]
    if not self.operation:
      msg_existing = (
        'Trying to load progress but no cluster being created. One reason '
         f'might be that a cluster named {self.clustername()} already exists '
         'and it was not spawned from this Dataproc Hub instance.')

      await yield_({'progress': 100, 'failed': True, 'message': msg_existing})
      self._raise_exception(msg_existing)

    resources = [f'projects/{self.project}']
    log_methods = {'doStart', 'instantiateMe', 'getOrCreateAgent', 'run',
                   'runBuiltinInitializationActions', 'awaitNameNodeSafeModeExit',
                   'runCustomInitializationActions'}
    filters_methods = ' OR '.join(f'"{method}"' for method in log_methods)

    filters_base = (
      f'resource.type=cloud_dataproc_cluster AND '
      f'resource.labels.cluster_name="{self.clustername()}" AND '
      f'resource.labels.cluster_uuid="{self.operation.metadata.cluster_uuid}" AND '
      f'log_name="projects/{self.project}/logs/google.dataproc.agent" AND '
      f'labels."compute.googleapis.com/resource_name"="{self.clustername()}-m"'
      f' AND jsonPayload.method=({filters_methods})'
    )
    filters = f'{filters_base} AND timestamp>="{spawner_progressor.start}"'
    self.log.debug(f'Filters are: {filters}')

    operation_done = False
    while not operation_done:
      try:
        operation_done = self.operation.done()
      except exceptions.GoogleAPICallError as e:
        self.log.warning(f'Error operation.done(): {e.message}')

      if not operation_done:
        await asyncio.sleep(10)

      try:
        self.log.info(
            f'# {self.clustername()}: {spawner_progressor.bar}%, fetching logs.')
        for entry in self.logging_client.list_log_entries(resources, filter_=filters):
          if entry.insert_id not in spawner_progressor.logging:
            payload = MessageToDict(entry.json_payload)
            message = f'{payload.get("method")}: {payload.get("message")}'
            spawner_progressor.bar += math.ceil((90 - spawner_progressor.bar) / 4)
            spawner_progressor.logging.add(entry.insert_id)
            tmp_yield = {'progress': spawner_progressor.bar, 'message': message}
            spawner_progressor.yields.append(tmp_yield)
            await yield_(tmp_yield)

      except (exceptions.GoogleAPICallError, exceptions.RetryError) as e:
        await yield_({'progress': spawner_progressor.bar, 'message': e.message})
        continue

      except ValueError:
        await yield_({'progress': spawner_progressor.bar, 'message': 'ValueError'})
        continue

    if self.operation.metadata.status.inner_state == 'FAILED':
      await yield_({
        'progress': 100,
        'failed': True,
        'message': f'FAILED: {self.operation.operation.error.message}'})

    if self.operation.metadata.status.inner_state == 'DONE':
      status = await self.get_cluster_status(self.clustername())
      if status == ClusterStatus.State.RUNNING:
        await yield_({
          'progress': 95,
          'message': 'Cluster successfully created'})

  ##############################################################################
  # User form functions
  ##############################################################################
  def _options_form_default(self):
    """ Builds form using values passed by administrator either in Terraform
    or in the jupyterhub_config_tpl.py file.
    """
    base_html = get_base_cluster_html_form(
        self._list_gcs_files(self.dataproc_configs),
        self.dataproc_zones,
        self.region
    )

    html_customize_cluster = ''
    if self.allow_custom_clusters:
      html_customize_cluster = get_custom_cluster_html_form(
          self._get_autoscaling_policy(),
          self.machine_types_list.split(',')
      )

    return '\n'.join([
        base_html,
        html_customize_cluster
    ])

  def _list_gcs_files(self, gcs_paths, sep=',', sort=True):
    """ Lists the file names of a GCS bucket or subfolder.
    Args:
    - str gcs_files: String that represents a path or a list of paths separated
      by `sep`. Path can be GCS buckets, subfolders or a list of files. Path can
      include gs:// and a trailing /.
    - str extension: if provided, includes only the files that ends with this
      string.
    """
    config_paths = []
    if gcs_paths:
      for path in gcs_paths.split(sep):
        path = self._clean_gcs_path(path, return_gs=False)
        gcs_bucket = path.split('/')[0]
        gcs_prefix = '/'.join(path.split('/')[1:])
        try:
          config_paths += [
              f'{gcs_bucket}/{b.name}' for b in
              self.gcs_client.list_blobs(gcs_bucket, prefix=gcs_prefix)]
        except exceptions.NotFound:
          pass

    config_paths = list(set(config_paths))
    if sort:
      config_paths = sorted(config_paths)
    return config_paths if config_paths else ''

  async def get_options_form(self):
    """ Overwrites default function in order to have a dynamic form which allows
    the update of dropdowns when the configs GCS location content changes for
    example.
    """
    return self._options_form_default()

  def options_from_form(self, formdata):
    """ Returns the selected option selected by the user.

    If authuser is in the GET url, it also sends the form.
    """
    self.log.info(f'formdata is {formdata}')

    options = {}
    for key, value in formdata.items():
      if value and isinstance(value, list):
        value = value[0]
      else:
        value = None
      if key == 'cluster_zone':
        self.zone = value or self.zone
      options[key] = value
    return options

################################################################################
# Overwrite
################################################################################
  def get_env(self):
    """ Sets env variables for spawner server.

    Overwrites the original function to overwrite some values and pass them to
    the Dataproc cluster through dataproc:jupyter.hub.env. Most JupyterHub values
    are now obsolete due to the redirect to prevent Dataproc cluster to be able
    to communicate back to JupyterHub.
    """
    env = super().get_env()
    remove_env =  [
      'JUPYTERHUB_API_TOKEN',
      'JPY_API_TOKEN',
      'JUPYTERHUB_CLIENT_ID',
      'JUPYTERHUB_OAUTH_CALLBACK_URL',
      'JUPYTERHUB_API_URL',
    ]
    for e in remove_env:
      env[e] = ''
    self.log.debug(f'env is {env}')
    return env

  def get_args(self):
    """Set arguments to pass to Jupyterhub-singleuser on the cluster.

    Note that we don't call the superclass method, as we don't want to
    set default args like port and ip.
    """
    args = []

    if self.debug:
      args.append('--debug')
    args.append('--SingleUserNotebookApp.hub_activity_interval=0')
    args.append('--SingleUserNotebookApp.hub_host={}'.format(self.hub_host))
    args.extend(self.args)
    return args

################################################################################
# Custom Functions
################################################################################
  def _raise_exception(self, msg):
    """ Creates an exception with a message that displays on spawn-pending. """
    e = web.HTTPError(500, msg)
    e.jupyterhub_message = msg
    raise e

  def get_dataproc_master_fqdn(self):
    """ Zonal DNS is in the form [CLUSTER NAME]-m.[ZONE].c.[PROJECT ID].internal
    If the project is domain-scoped, then PROJECT ID needs to be in the form
    [PROJECT NAME].[DOMAIN].
    More info here:
    https://cloud.google.com/compute/docs/internal-dns#instance-fully-qualified-domain-names

    Returns
      String: the FQDN of the master node.
    """
    if ':' in self.project:
      # Domain-scoped project
      domain_name, domain_project = self.project.split(':')
      return f'{self.clustername()}-m.{self.zone}.c.{domain_project}.{domain_name}.internal'
    else:
      return f'{self.clustername()}-m.{self.zone}.c.{self.project}.internal'

  def camelcase_to_snakecase(self, cc):
    """ Converts yaml's keys from CamelCase to snake_case so the cluster config
        is understandable by the Dataproc's Python client. """

    # 1. Changes the first aA starting from line beginning to a_A.
    # 2. Changes all the ones after and stops at the first :
    # 3. Lower case all the _A
    sc = re.sub('(^[_a-z \t-]*)([a-z])([A-Z])', r'\1\2_\3', cc)
    sc = re.sub('([a-z])([A-Z])(?=.+:)', r'\1_\2', sc)
    sc = re.sub('([a-zA-Z0-9_]+):', lambda m: m.group(0).lower(), sc)
    return sc

  def read_gcs_file(self, file_path) -> dict:
    if file_path:
      file_path = file_path.replace('gs://', '').replace('//', '/').split('/')
    bn = file_path[0]
    fp = '/'.join(file_path[1:])

    working_bucket = self.gcs_client.get_bucket(bn)
    config_blob = working_bucket.get_blob(fp)
    config_string = config_blob.download_as_string()
    config_string = config_string.decode('utf-8')
    return config_string

  def get_cluster_definition(self, file_path):
    """ Returns the content of a GCS file

    Usage: file_path('mybucket/subfolder/filename.yaml'). Make sure that
    there is not comment in the yaml file. Find Dataproc properties here:
    https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/ClusterConfig
    https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties

    Args:
      String file_path: path to the file to read. Includes bucket and folders in the bucket
    Returns:
      (bytes, dict): Content of the file both as a string and yaml dict.
    """
    config_string = self.read_gcs_file(file_path)
    config_dict = yaml.load(config_string, Loader=yaml.FullLoader)
    config_dict.setdefault('config', {})

    # Properties and Metadata might have some values that needs to remain with
    # CamelCase so we remove the properties/metadata from the conversion from
    # CamelCase to snake_case and add the properties/metadata back afterwards.
    skip_properties = {}
    skip_metadata = {}

    if 'properties' in config_dict['config'].setdefault('softwareConfig', {}):
      skip_properties = config_dict['config']['softwareConfig']['properties']
      del config_dict['config']['softwareConfig']['properties']

    if 'metadata' in config_dict['config'].setdefault('gceClusterConfig', {}):
      skip_metadata = config_dict['config']['gceClusterConfig']['metadata']
      del config_dict['config']['gceClusterConfig']['metadata']

    config_string = yaml.dump(config_dict)
    config_string = self.camelcase_to_snakecase(config_string)
    config_dict = yaml.load(config_string, Loader=yaml.FullLoader)

    if skip_properties:
      config_dict['config']['software_config']['properties'] = skip_properties

    if skip_metadata:
      config_dict['config']['gce_cluster_config']['metadata'] = skip_metadata

    self.log.debug(f'config_dict is {config_dict}')
    return config_dict

  def create_example_notebooks(self):
    if not self.gcs_user_folder:
      return
    default_path = self.default_notebooks_gcs_path
    user_folder = self.gcs_user_folder
    if not default_path or not user_folder:
      self.log.debug('Nothing to copy')
      return
    storage_client = storage.Client(project=self.project)
    bucket_name, folder_name = self._split_gcs_path(default_path)
    destination_bucket_name, destination_folder_name = self._split_gcs_path(user_folder)
    destination_folder_name += self.default_notebooks_folder
    self.log.debug(
        f'Copy from {bucket_name}/{folder_name} to '
        f'{destination_bucket_name}/{destination_folder_name}')

    source_bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name)
    for blob in blobs:
      if blob.name == folder_name:
        continue
      source_bucket.copy_blob(
          source_bucket.blob(blob.name),
          storage_client.bucket(destination_bucket_name),
          destination_folder_name + blob.name[len(folder_name):]
      )

  async def create_cluster(self):
    """ Creates a cluster using templated yaml file. """
    self.log.info(f'Creating cluster with name {self.clustername()}')

    # Dumps the environment variables, including those generated after init.
    # (ex. JUPYTERHUB_API_TOKEN)
    self.temp_env = self.get_env()
    self.temp_env['PATH'] = '/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' # pylint: disable=line-too-long
    self.env_str = json.dumps(self.temp_env)
    self.args_str = ' '.join(self.get_args())

    # Even if an admin provides a definition, ensures it is properly formed.
    self.cluster_definition = self._build_cluster_config(self.cluster_data)
    cluster = await self._create_cluster(self.cluster_definition)
    return cluster

  async def _create_cluster(self, cluster_data, try_count=3):
    """ Implements custom retry functionality in order to change zone before
    each recall. """
    while True:
      try:
        # Prevents having a ERROR:asyncio:Task exception was never retrieved.
        await asyncio.sleep(0)
        return self.dataproc_client.create_cluster(project_id=self.project,
                                                   region=self.region,
                                                   cluster=cluster_data)
      except exceptions.InvalidArgument as e:
        self._raise_exception(e.message)
      except (exceptions.PermissionDenied,
              exceptions.TooManyRequests,
              exceptions.ResourceExhausted) as e:

        if (await self.get_cluster_status(self.clustername())
            == ClusterStatus.State.DELETING):
          self.log.warning(f'Cluster {self.clustername()} pending deletion.')

        try_count -= 1
        if try_count > 0:
          cluster_data = self.change_zone_in_cluster_cfg(cluster_data)
          await asyncio.sleep(3)
          continue
        raise e

  def change_zone_in_cluster_cfg(self, cluster_data):
    current_zone_tmp = cluster_data['config']['gce_cluster_config']['zone_uri'].split('/')[-1]
    current_zone = current_zone_tmp[-1]
    new_zone = None

    locations_list = self.dataproc_locations_list.split(',')
    zones_list = []
    for zone_letter in locations_list:
      tmp_zone = f'{self.region}-{zone_letter}'
      if zone_letter != current_zone:
        new_zone = tmp_zone
      zones_list.append(tmp_zone)
    if not new_zone:
      if zones_list:
        new_zone = random.choice(zones_list)
      else:
        new_zone = current_zone_tmp

    cluster_data['config']['gce_cluster_config']['zone_uri'] = (
        'https://www.googleapis.com/compute/v1/projects/'
        f'{self.project}/zones/{new_zone}')
    return cluster_data

  async def get_cluster_notebook_endpoint(self, clustername):
    """ Extracts Component Gateway endpoint for Jupyter on the cluster.

    Returns:
      (str) Component Gateway URL without the trailing `/`
    """
    endpoint_name = 'JupyterLab' if 'lab' in self.default_url else 'Jupyter'
    cluster = await self.get_cluster(clustername)
    if cluster is None:
      self._raise_exception(f'Can not get cluster {clustername} information.')
    if not cluster.config.endpoint_config:
      self._raise_exception(f'No endpoint_config set for {clustername}.')
    if endpoint_name not in cluster.config.endpoint_config.http_ports:
      self._raise_exception(f'No Jupyter endpoint configured for {clustername}.')
    return cluster.config.endpoint_config.http_ports[endpoint_name].rstrip('/')

  async def get_cluster_status(self, clustername):
    cluster = await self.get_cluster(clustername)
    if cluster is not None:
      return cluster.status.state
    return None

  async def exists(self, clustername):
    return (await self.get_cluster(clustername)) is not None

  async def get_cluster(self, clustername):
    try:
      return self.dataproc_client.get_cluster(
          project_id=self.project,
          region=self.region,
          cluster_name=clustername)
    except exceptions.NotFound:
      return None
    except (exceptions.GoogleAPICallError, exceptions.PermissionDenied) as e:
      self.log.info(f'# get_cluster exception: {e.message}')
      return None

################################################################################
# Helper Functions
################################################################################

  def get_username(self, raw=False):
    username, _ = (self.user.name.split('@') + [None])[:2]
    return username if raw else re.sub(r'[^a-zA-Z0-9=]', '-', str(username))

  def clustername(self, cluster_name=None):
    """ JupyterHub provides a notebook per user, so the username is used to
    distinguish between clusters. """
    if cluster_name is None:
      cluster_name = self.cluster_name_pattern.format(self.get_username())
      if self.name:
        cluster_name += f'-{self.name}'
      # Set only if allow_named_servers == True
      if self.rand_str:
        cluster_name += self.rand_str
    return cluster_name.lower()

  def calculate_config_value(self, key, path):
    """ Checks if a key exists at a dictionary path and returns a default value
    if not. Otherwise, returns the value there.
    key:  ie 'zone_uri'
    path: ie cluster_data['config']['gce_cluster_config']
    default:
    """
    if key not in path:
      return '<TODO JUPYTERHUB ONE>'

    return path[key]

  def _is_idle_checker_enable(self):
    self.log.debug(f'Idle checker settings: {self.idle_checker}')
    return (self.idle_checker and
            self.idle_checker.get('idle_job_path') and
            self.idle_checker.get('idle_path'))

  def _split_gcs_path(self, path: str):
    gcs_prefix = 'gs://'
    if path.startswith(gcs_prefix):
      path = path[len(gcs_prefix):]
    path_parts = path.split('/')
    bucket = path_parts[0]
    folder = '/'.join(path_parts[1:])
    if not folder.endswith('/'):
      folder += '/'
    return bucket, folder

  def _clean_gcs_path(self, gcs_path, return_gs=True, return_slash=False):
    """ Takes a GCS path starting with or without gs:// and returns a consistent
    value.
    Args:
      - str gcs_path: a GCS path URI that starts with gs:// or not.
      - bool return_gs: if True returns a string starting with gs://, otherwise
      returns a GCS path that starts directly with the bucket name. """
    gcs_starter = 'gs://'
    if (gcs_path.startswith(gcs_starter)) and (not return_gs):
      gcs_path = gcs_path[len(gcs_starter):]
    if (gcs_path.endswith('/')) and (not return_slash):
      gcs_path = gcs_path[:-1]
    return gcs_path

  def convert_string_to_duration(self, data):
    """ A cluster export exports times as string using the JSON API but creating
    but a cluster uses Duration protobuf. This function checks if the fields
    known to be affected by this behavior are present in the cluster config.
    If so, it changes their value from string to a Duration in YAML. Duration
    looks like {'seconds': 15, 'nanos': 0}. """
    def to_sec(united):
      """ Converts a time string finishing by a time unit as the matching number
      of seconds. """
      if united:
        time_span = united[:-1]
        time_unit = united[-1]
        if time_unit == 'm':
          return int(time_span) * 60
        elif time_unit == 'h':
          return int(time_span) * 3600
        elif time_unit == 'd':
          return int(time_span) * 86400
        else:
          return int(time_span)
      return united

    # Loops through initialization actions list and replace values that have
    if data['config'].setdefault('initialization_actions', []):
      idx = 0
      for init_action in data['config']['initialization_actions']:
        if ('execution_timeout' in init_action
            and isinstance(init_action['execution_timeout'], str)):
          data['config']['initialization_actions'][idx]['execution_timeout'] = {
              'seconds': to_sec(init_action['execution_timeout']),
              'nanos': 0
          }
        idx = idx + 1

    # Converts durations for lifecycle_config.
    if ('idle_delete_ttl' in data['config'].setdefault('lifecycle_config', {})
        and isinstance(data['config']['lifecycle_config']['idle_delete_ttl'], str)):
      data['config']['lifecycle_config']['idle_delete_ttl'] = {
          'seconds': to_sec(data['config']['lifecycle_config']['idle_delete_ttl']),
          'nanos': 0
      }

    if ('auto_delete_ttl' in data['config'].setdefault('lifecycle_config', {})
        and isinstance(data['config']['lifecycle_config']['auto_delete_ttl'], str)):
      data['config']['lifecycle_config']['auto_delete_ttl'] = {
          'seconds': to_sec(data['config']['lifecycle_config']['auto_delete_ttl']),
          'nanos': 0
      }

    return data.copy()

  def _check_uri_geo(self, uri, uri_geo_slice, expected_geo, trim_zone=False):
    uri_geo = None
    uri_data = uri.split('/')
    if len(uri_data) > 1:
      uri_geo = uri_data[uri_geo_slice]
      if trim_zone:
        uri_geo = uri_geo[:-2]
      if uri_geo not in [expected_geo, 'global']:
        self._raise_exception(
            f'The location {uri_geo} of the uri {uri} in the yaml file does '
            f'not match the Dataproc Hub one {expected_geo}. Please contact '
            'your admnistrator.')
    return uri_geo or uri

  def _image_version_supports_anaconda(self, image_version):
    """Validate whether given image version supports Anaconda.

    Preview images after 2.0.0-RC12 do not support Anaconda.
    For older versions, we force Anaconda to be activated in
    order for Jupyter to work correctly."""
    parts = image_version.split('-')

    if parts[0].lower() == 'preview':
      return False
    elif parts[0] == '2.0.0':
      if len(parts) < 3:
        return False
      if parts[1] in [f'RC{i}' for i in range(1, 12)]:
        return True

    try:
      major_version = int(parts[0].split('.')[0])
    except ValueError as e:
      self.log.warning('Failed to parse image version "%s": %s' % (image_version, e))
      # Something weird is going on with image version format, fail open
      return True

    return major_version < 2

  def _list_to_dict(self, rlist):
    """ Converts list of user defined labels to dictionary. """
    return dict(map(lambda s: s.split(':'), rlist))

  def _get_rand_string(self, length):
    """ Generates a fixed length random alphanumeric string of lower letters
    and digits.
    """
    letters_and_digits = string.ascii_lowercase + string.digits
    rand_str = ''.join((random.choice(letters_and_digits) for i in range(length)))
    return rand_str

  def _validate_zones(self, region, zones):
    """ Checks that given zone letters are in a given region.

    Args:
      region (str): a region name similar to us-central1
      zones (str): a comma-separated list of zones letters.
    Returns:
      If zones letters are provided, returns letters that exist in the region.
      If no zones letters are provided, returns all the letters for the region.
    """
    zone_filter = (
        f'region eq '
        f'https://www.googleapis.com/compute/v1/projects/{self.project}/'
        f'regions/{region}')
    try:
      request = self.compute_client.zones().list(project=self.project, filter=zone_filter)
      response = request.execute()
      region_zone_letters = [z['name'][-1].strip() for z in response['items']]
    except Exception as e: ## pylint: disable=broad-except
      self.log.info(f'Failed to validate Dataproc locations list:\n\t{e}')

    if not zones:
      return region_zone_letters

    return [z for z in zones.split(',') if z in region_zone_letters] or region_zone_letters

  def _get_image_version(self, custom_image):
    project = custom_image.split('/')[1]
    image = custom_image.split('/')[4]
    goog_version = ''
    try:
      request = self.compute_client.images().get(project=project, image=image)
      response = request.execute()
      version = re.split(r'(-debian.*|-ubuntu.*)', response['labels']['goog-dataproc-version'])
      goog_version = version[0].replace('-', '.') + version[1]
    except Exception as e: ## pylint: disable=broad-except
      self.log.info(f'Failed to process Dataproc custom image version:\n\t{e}')

    return goog_version


################################################################################
# Cluster configuration
################################################################################

  def _get_autoscaling_policy(self) -> list:
    """
    Get all autopscaling policies for dataproc.
    Method use bash command 'gcloud'
    :return: list of auto scaling policies
    """
    command = f'gcloud beta dataproc autoscaling-policies list --region {self.region}'
    a = os.popen(command)
    is_exists = False
    res = []
    for i in a:
      i = i.replace('\n', '')
      if not is_exists:
        if i == 'ID':
          is_exists = True
        else:
          break
      else:
        res.append(i)
    self.log.debug('Available autoscaling policies res')
    return res

  def _apply_users_configs(self, cluster_data):

    config = cluster_data['config']
    config.setdefault('initialization_actions', [])

    if self.user_options.get('pip_packages'):
      config.setdefault('gce_cluster_config', {})
      config['gce_cluster_config'].setdefault('metadata', {})
      config['gce_cluster_config']['metadata'].setdefault('PIP_PACKAGES', '')
      pip_packages = set(filter(None, set(
          config['gce_cluster_config']['metadata']['PIP_PACKAGES'].split(' ')
          + self.user_options.get('pip_packages').split(' '))))
      config['gce_cluster_config']['metadata']['PIP_PACKAGES'] = ' '.join(pip_packages)
      config['initialization_actions'].append(
          {
              'executable_file': 'gs://dataproc-initialization-actions/python/pip-install.sh'
          }
      )

    if self.user_options.get('conda_packages'):
      config.setdefault('gce_cluster_config', {})
      config['gce_cluster_config'].setdefault('metadata', {})
      config['gce_cluster_config']['metadata'].setdefault('CONDA_PACKAGES', '')
      conda_packages = set(filter(None, set(
          config['gce_cluster_config']['metadata']['CONDA_PACKAGES'].split(' ')
          + self.user_options.get('conda_packages').split(' '))))
      config['gce_cluster_config']['metadata']['CONDA_PACKAGES'] = ' '.join(conda_packages)
      config['initialization_actions'].append(
          {
              'executable_file': 'gs://dataproc-initialization-actions/python/conda-install.sh'
          }
      )

    if self.user_options.get('init_actions'):
      for init_action in self.user_options.get('init_actions').split(','):
        if init_action.startswith('gs://'):
          config['initialization_actions'].append(
            {
              'executable_file': init_action
            }
          )

    if self.user_options.get('cluster_props_prefix_0'):
      props = {key: val for key, val in self.user_options.items() if 'cluster_props' in key}
      try:
        if len(props.keys()) % 3 == 0:
          for i in range(int(len(props.keys()) / 3)):
            prefix = props[f'cluster_props_prefix_{i}']
            key = props[f'cluster_props_key_{i}']
            value = props[f'cluster_props_val_{i}']
            config['software_config']['properties'][f'{prefix}:{key}'] = value
      except Exception as e: ## pylint: disable=broad-except
        self.log.info(f'Failed to process Dataproc cluster properties:\n\t{e}')

    if self.user_options.get('image_version'):
      if self.user_options.get('custom_image'):
        config['master_config']['image_uri'] = self.user_options.get('custom_image')
        config['worker_config']['image_uri'] = self.user_options.get('custom_image')
        config['software_config']['image_version'] = \
          self._get_image_version(self.user_options.get('custom_image'))
      else:
        config['master_config'].pop('image_uri', None)
        config['worker_config'].pop('image_uri', None)
        config['software_config']['image_version'] = self.user_options.get('image_version')

    if self.user_options.get('master_node_type'):
      config['master_config']['machine_type_uri'] = self.user_options.get('master_node_type')

    if self.user_options.get('worker_node_type'):
      config['worker_config']['machine_type_uri'] = self.user_options.get('worker_node_type')

    if self.user_options.get('master_disk_type'):
      config['master_config'].setdefault('disk_config', {})
      config['master_config']['disk_config']['boot_disk_type'] = \
        self.user_options.get('master_disk_type')

    if self.user_options.get('worker_disk_type'):
      config['worker_config'].setdefault('disk_config', {})
      config['worker_config']['disk_config']['boot_disk_type'] = \
        self.user_options.get('worker_disk_type')

    if self.user_options.get('master_disk_size'):
      try:
        val = int(self.user_options.get('master_disk_size'))
        config['master_config'].setdefault('disk_config', {})
        config['master_config']['disk_config']['boot_disk_size_gb'] = val
      except ValueError:
        pass

    if self.user_options.get('worker_disk_size'):
      try:
        val = int(self.user_options.get('worker_disk_size'))
        config['worker_config'].setdefault('disk_config', {})
        config['worker_config']['disk_config']['boot_disk_size_gb'] = val
      except ValueError:
        pass

    if self.user_options.get('worker_node_amount'):
      try:
        val = int(self.user_options.get('worker_node_amount'))
        config['worker_config']['num_instances'] = val
      except ValueError:
        pass

    if self.user_options.get('sec_worker_disk_type'):
      config.setdefault('secondary_worker_config', {})
      config['secondary_worker_config'].setdefault('disk_config', {})
      config['secondary_worker_config']['disk_config']['boot_disk_type'] = \
        self.user_options.get('sec_worker_disk_type')

    if self.user_options.get('sec_worker_disk_size'):
      try:
        val = int(self.user_options.get('sec_worker_disk_size'))
        config.setdefault('secondary_worker_config', {})
        config['secondary_worker_config'].setdefault('disk_config', {})
        config['secondary_worker_config']['disk_config']['boot_disk_size_gb'] = val
      except ValueError:
        pass

    if self.user_options.get('sec_worker_node_amount'):
      config.setdefault('secondary_worker_config', {})
      config['secondary_worker_config']['num_instances'] = \
        int(self.user_options.get('sec_worker_node_amount'))

    autoscaling_policy = self.user_options.get('autoscaling_policy', '')
    if autoscaling_policy and autoscaling_policy != 'None':
      cluster_data['config']['autoscaling_config'] = {
          'policy_uri': (
              f"""https://www.googleapis.com/compute/v1/projects/"""
              f"""{self.project}/locations/{self.region}/"""
              f"""autoscalingPolicies/{autoscaling_policy}""")
      }

    if self._is_custom_hive_settings():
      config['software_config']['properties']['hive:hive.metastore.schema.verification'] = 'false'
      config['software_config']['properties']['hive:javax.jdo.option.ConnectionURL'] = \
          f'jdbc:mysql://{self.user_options["hive_host"]}/{self.user_options["hive_db"]}'
      config['software_config']['properties']['hive:javax.jdo.option.ConnectionUserName'] = \
          self.user_options['hive_user']
      config['software_config']['properties']['hive:javax.jdo.option.ConnectionPassword'] = \
          self.user_options['hive_passwd']

    # To handle custom Java & Scala packages, use the following code to get values:
    # self.user_options.get('java_packages', '')
    # self.user_options.get('scala_packages', '')

    if self.user_options.get('custom_labels'):
      cluster_data.setdefault('labels', {})
      for key, val in self._list_to_dict(self.user_options.get('custom_labels').split(',')).items():
        cluster_data['labels'][key] = val

    return cluster_data

  def _is_custom_hive_settings(self):
    return self.user_options.get('hive_host') and self.user_options.get('hive_db') \
         and self.user_options.get('hive_user') and self.user_options.get('hive_passwd')

  def _build_cluster_config(self, cluster_data=None):
    """ Creates a cluster definition based on different inputs:
    1. Required data to start a dataproc cluster (name and project ID)
    2. Values chosen by an end user through a form if any.
    3. Admin-provided data through a YAML file (chosen by user or default one.)
    """
    # Default required values that can be overwritten by the YAML file content
    # but must be set in case there is no form.
    cluster_data = cluster_data or {}
    cluster_zone = self.zone
    exclusive_user_property = 'dataproc:dataproc.exclusive.user'

    # Sets the cluster definition with form data.
    if self.user_options:
      metadata = {}

      # Reads values chosen by the user in the form and overwrites any existing
      # ones if relevant.
      gcs_config_file = self.user_options.get('cluster_type')
      cluster_zone = self.user_options.get('cluster_zone')
      exclusive_user = self.user_options.get('exclusive_user')

      # Reads the cluster config from yaml
      self.log.info(f'Reading config file at {gcs_config_file}')
      cluster_data = self.get_cluster_definition(gcs_config_file)
      # Validate and strip unknown fields
      # TODO(dingj) expose warnings somehow
      warnings = _validate_proto(cluster_data, Cluster)
      self.log.debug(f'Cluster config after cleaning was {cluster_data}')
      if warnings:
        self.log.info(f'Warnings from proto field validation were {warnings}')

      # Defines default values if some key is not exists
      cluster_data['config'].setdefault('gce_cluster_config', {})
      cluster_data['config'].setdefault('master_config', {})
      cluster_data['config'].setdefault('worker_config', {})
      cluster_data['config'].setdefault('initialization_actions', [])
      cluster_data['config'].setdefault('software_config', {})
      cluster_data['config']['software_config'].setdefault('properties', {})

      if 'metadata' in cluster_data['config']['gce_cluster_config']:
        metadata = cluster_data['config']['gce_cluster_config']['metadata']

      # User checked the box to limit access to CG URL.
      if exclusive_user == 'on':
        (cluster_data['config']['software_config']['properties']
                     [exclusive_user_property]) = self.user.name

      # Sets default network for the cluster if not already provided in YAML.
      if ('network_uri' not in cluster_data['config']['gce_cluster_config'] and
          'subnetwork_uri' not in cluster_data['config']['gce_cluster_config']):
        if self.dataproc_default_subnet:
          (cluster_data['config']['gce_cluster_config']
           ['subnetwork_uri']) = self.dataproc_default_subnet

      # Cluster identity and scopes
      if 'service_account' not in cluster_data['config']['gce_cluster_config']:
        if self.dataproc_service_account:
          (cluster_data['config']['gce_cluster_config']
           ['service_account']) = self.dataproc_service_account
          (cluster_data['config']['gce_cluster_config']
           ['service_account_scopes']) = [
               'https://www.googleapis.com/auth/cloud-platform']

      init_actions = []
      if self._is_idle_checker_enable():
        init_actions.append(
            {
                'executable_file': self.idle_checker.get('idle_job_path'),
                'execution_timeout': {'seconds': 1800, 'nanos': 0}
            }
        )
        idle_path = self.idle_checker.get('idle_path')
        if idle_path.endswith('isIdle.sh'):
          idle_path = idle_path[:-len('isIdle.sh')]
        if idle_path.endswith('/'):
          idle_path = idle_path[:-len('/')]
        metadata['script_storage_location'] = idle_path
        metadata['max-idle'] = self.idle_checker.get('timeout', '60m')

      metadata['session-user'] = self.get_username()
      cluster_data['config']['gce_cluster_config']['metadata'] = metadata
      cluster_data['config']['initialization_actions'] = (
          init_actions + cluster_data['config']['initialization_actions']
      )

      if 'labels' not in cluster_data:
        cluster_data.setdefault('labels', {})

      if self.allow_custom_clusters and self.user_options.get('custom_cluster'):
        cluster_data = self._apply_users_configs(cluster_data)

      # Apply label to tag which host environment this cluster was spawned from
      cluster_data.setdefault('labels', {})
      cluster_data['labels']['goog-dataproc-notebook-spawner'] = (
          self.spawner_host_type.lower() if self.spawner_host_type != '' else 'unknown'
      )

    # Always override project id and name
    cluster_data['project_id'] = self.project
    cluster_data['cluster_name'] = self.clustername()
    cluster_data.setdefault('config', {})

    # Sets the zone. Which one overwrites is decided in the form logic.
    cluster_data['config'].setdefault('gce_cluster_config', {})
    cluster_data['config']['gce_cluster_config']['zone_uri'] = (
        f'https://www.googleapis.com/compute/v1/projects/{self.project}/'
        f'zones/{cluster_zone}')

    # Overwrites some existing data with required values.
    cluster_data['config'].setdefault('software_config', {})
    cluster_data['config']['software_config'].setdefault('properties', {})
    cluster_data['config'].setdefault('master_config', {})

    (cluster_data['config']['software_config']['properties']
                 ['dataproc:jupyter.hub.args']) = self.args_str
    (cluster_data['config']['software_config']['properties']
                 ['dataproc:jupyter.hub.env']) = self.env_str
    (cluster_data['config']['software_config']['properties']
                 ['dataproc:jupyter.hub.menu.enabled']) = 'true'
    (cluster_data['config']['software_config']['properties']
                 .pop('dataproc:jupyter.hub.enabled', None))
    if not self.show_spawned_clusters_in_notebooks_list:
      (cluster_data['config']['software_config']['properties']
                   ['dataproc:jupyter.instance-tag.enabled']) = 'false'

    if self.gcs_user_folder:
      (cluster_data['config']['software_config']['properties']
       ['dataproc:jupyter.notebook.gcs.dir']) = self.gcs_user_folder

    if 'image_version' not in cluster_data['config']['software_config']:
      if 'image_uri' in cluster_data['config']['master_config']:
        cluster_data['config']['software_config']['image_version'] = \
          self._get_image_version(cluster_data['config']['master_config']['image_uri'])
      else:
        cluster_data['config']['software_config']['image_version'] = '1.4-debian9'

    # A user can only set 'dataproc:dataproc.exclusive.user' for themselves.
    # Otherwise the CG Url is not accessible by either identities. So, if set,
    # the exclusive auth property can only have the value of self.user.name. For
    # that reason, there is no priority between yaml and user.
    if (self.force_single_user or
        exclusive_user_property in cluster_data['config']['software_config']['properties']):
      (cluster_data['config']['software_config']['properties']
                   [exclusive_user_property]) = self.user.name

    # Forces Component Gateway
    cluster_data['config'].setdefault('endpoint_config', {})
    cluster_data['config']['endpoint_config']['enable_http_port_access'] = True

    cluster_data['config']['software_config'].setdefault('optional_components', [])
    # Converts component's string to its int value (See Component protobuf in
    # google-cloud-dataproc library). This allows to pass strings in yaml.
    optional_components = {
        Component[c].value if isinstance(c, str) else c for
        c in cluster_data['config']['software_config']['optional_components']
    }

    if self.force_add_jupyter_component:
      if Component['JUPYTER'].value not in optional_components:
        optional_components.add(Component['JUPYTER'].value)
      # preview (2.x) images and up do not support Anaconda
      if self._image_version_supports_anaconda(
          cluster_data['config']['software_config']['image_version']):
        if Component['ANACONDA'].value not in optional_components:
          optional_components.add(Component['ANACONDA'].value)
      else:
        if Component['ANACONDA'].value in optional_components:
          optional_components.remove(Component['ANACONDA'].value)

    cluster_data['config']['software_config']['optional_components'] = (
        list(optional_components)
    )

    # Ensures that durations match the Protobuf format ({seconds:300, nanos:0})
    cluster_data = self.convert_string_to_duration(cluster_data.copy())

    # Checks that cluster subnet location matches with the Hub's one.
    # Must support all string patterns for subnetworkUri:
    # https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig
    if 'subnetwork_uri' in cluster_data['config']['gce_cluster_config']:
      self._check_uri_geo(
          uri=cluster_data['config']['gce_cluster_config']['subnetwork_uri'],
          uri_geo_slice=-3,
          expected_geo=self.region
      )

    for server_group in ['master_config', 'worker_config', 'secondary_worker_config']:
      if server_group in cluster_data['config']:
        # We do not check the zone because the user form overwrites it.
        # MachineTypes and Accelerators must be in the same zone as the Dataproc
        # Cluster. Removes the zone reference if YAML provides a full uri.
        if 'machine_type_uri' in cluster_data['config'][server_group]:
          cluster_data['config'][server_group]['machine_type_uri'] = (
              cluster_data['config'][server_group]['machine_type_uri']
              .split('/')[-1]
          )
        # Accelerator types must be in the same zone as the Dataproc Cluster.
        if 'accelerators' in cluster_data['config'][server_group]:
          for acc_idx, acc_val in enumerate(
              cluster_data['config'][server_group]['accelerators']):
            (cluster_data['config'][server_group]['accelerators'][acc_idx]
             ['accelerator_type_uri']) = (
                 acc_val['accelerator_type_uri'].split('/')[-1]
             )

    # Strip cluster-specific namenode properties
    if (cluster_data['config'].setdefault('software_config', {}) and
        cluster_data['config']['software_config'].setdefault('properties', {})):
      cluster_data['config']['software_config']['properties'].pop(
          'hdfs:dfs.namenode.lifeline.rpc-address', None)
      cluster_data['config']['software_config']['properties'].pop(
          'hdfs:dfs.namenode.servicerpc-address', None)

    self.log.info(f'Cluster configuration data is {cluster_data}')
    return cluster_data
