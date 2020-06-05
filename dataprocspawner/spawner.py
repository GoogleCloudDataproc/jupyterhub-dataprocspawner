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

import json
import re
import os
import time
import random

import yaml
from jupyterhub.spawner import Spawner
from google.api_core import exceptions
from google.cloud import dataproc_v1beta2
from google.cloud.dataproc_v1beta2.gapic.transports import (
    cluster_controller_grpc_transport)
from google.cloud.dataproc_v1beta2.gapic.transports import (
    job_controller_grpc_transport)
from google.cloud.dataproc_v1beta2.gapic.enums import ClusterStatus
from google.cloud import storage
from traitlets import List, Unicode, Tuple, Dict, Bool
from google.protobuf.internal.well_known_types import Duration

from .customize_cluster import get_base_cluster_html_form
from .customize_cluster import get_custom_cluster_html_form

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


class DataprocSpawner(Spawner):
  """Spawner for Dataproc clusters.

  Reference: https://jupyterhub.readthedocs.io/en/stable/reference/spawners.html
  """
  
  poll_interval = 5

  # Since creating a cluster takes longer than the 30 second default,
  # up this value so Jupyterhub can connect to the spawned server.
  # Unit is in seconds.
  http_timeout = 900

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
      Comma separated list of the dataproc configurations available in the user spawning form.
      Each value should contain the top bucket name (can be without gs://) and path to the files from
      that bucket name. 
      
      Example 1 config file: 'bucket/configs/file.yaml' or the same value 'gs://bucket/configs/file.yaml'
      Example 2 config files: 'bucket/configs/file1.yaml,bucket/configs/file2.yaml

      This must be configured
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
      "",
      config=True,
      help=""" 
      Comma separated list of the zone letters where to spawn Cloud Dataproc in
      the JupyterHub region.
      Example: "a,b"

      This must be configured.
      """,)

  idle_checker = Dict(
      {"idle_job_path": "", "idle_path": "", "timeout": "60m"},
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
    help="The name of folder into which service will copy default notebooks",)

  machine_types_list = Unicode(
    '',
    config=True,
    help="Allowed machine types",)

  # Overwrites the env_keep from Spawner to only include PATH and LANG
  env_keep = List(
      [
        'PATH',
        'LANG',
      ],
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
      help="Host type on which the Spawner is running (e.g. gce, ain)",)

  force_add_jupyter_component = Bool(
      True,
      config=True,
      help="""
      Whether to always enable the JUPYTER and ANACONDA optional components
      even if not explicitly specified in the cluster config. It is recommended
      to set this to True, as clusters without these components will *not* function
      correctly when spawned.
      """,)

  def __init__(self, *args, **kwargs):
    _mock = kwargs.pop('_mock', False)
    super().__init__(*args, **kwargs)

    if _mock:
      # Mock the API
      self.dataproc_client = kwargs.get('dataproc')
      self.gcs_client = kwargs.get('gcs')
    else:
      self.client_transport = (
        cluster_controller_grpc_transport.ClusterControllerGrpcTransport(
            address=f'{self.region}-dataproc.googleapis.com:443'))
      self.dataproc_client = dataproc_v1beta2.ClusterControllerClient(
          self.client_transport)
      self.gcs_client = storage.Client(project=self.project)

    if self.gcs_notebooks:
      if self.gcs_notebooks.startswith('gs://'):
        self.gcs_notebooks = self.gcs_notebooks[5:]
      
      self.gcs_user_folder = f'gs://{self.gcs_notebooks}/{self.get_username()}'
      
  
################################################################################
# Required functions
################################################################################
  async def start(self):
    """ Creates a Dataproc cluster.
    If a cluster with the same name already exists, logs a warning and returns 
    the same values as if creating the cluster.
    
    Returns:
      (String, Int): FQDN of the master node and the port it's accessible at.
    """
    if await self.get_cluster_status(self.clustername()) == ClusterStatus.State.DELETING:
      raise RuntimeError(f'Cluster {self.clustername()} is pending deletion.')
    
    elif await self.exists(self.clustername()):
      self.log.warning(f'Cluster named {self.clustername()} already exists')
    
    else:
      if self.gcs_user_folder:
        self.create_example_notebooks()
      await self.create_cluster()

      start_notebook_cmd = self.cmd + self.get_args()
      start_notebook_cmd = ' '.join(start_notebook_cmd)
      self.log.info(start_notebook_cmd)
    
    return (self.getDataprocMasterFQDN(), self.port)

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
    if status is None or status in (ClusterStatus.State.ERROR, 
                                    ClusterStatus.State.DELETING, 
                                    ClusterStatus.State.UNKNOWN):
      return 1
    elif status == ClusterStatus.State.CREATING:
      self.log.info(f'{self.clustername()} is creating')
      return None
    elif status in (ClusterStatus.State.RUNNING, ClusterStatus.State.UPDATING):
      self.log.info(f'{self.clustername()} is up and running')
      return None

################################################################################
# User form functions
################################################################################
  def _options_form_default(self):
    """ Builds form using values passed by administrator either in Terraform
    or in the jupyterhub_config_tpl.py file.
    """
    
    # Skips the form if no config provided.
    if not self.dataproc_configs:
      return ""

    base_html = get_base_cluster_html_form(
      self.dataproc_configs.split(','),
      self.dataproc_locations_list.split(','),
      self.region
    )

    html_customize_cluster = ""
    if self.allow_custom_clusters:
      html_customize_cluster = get_custom_cluster_html_form(
        self._get_autoscaling_policy(),
        self.machine_types_list.split(',')
      )

    return "\n".join([
      base_html,
      html_customize_cluster
    ])

  def options_from_form(self, formdata):
    """ 
    Returns the selected option selected by the user. It sets self.user_options.
    """
    self.log.info(f'''formdata is {formdata}''')

    options = {}
    for key, value in formdata.items():
      if value:
        if isinstance(value, list):
          value = value[0]
        else:
          value = value
      else:
        value = None

      if key == "cluster_zone":
        self.zone = value or self.zone

      options[key] = value

    self.log.info(f'''User selected cluster: {options.get('cluster_type')} 
          and zone: {self.zone} in region {self.region}.''')

    return options

################################################################################
# Overwrite
################################################################################
  def get_env(self):
    """ Overwrites the original function to get a new Hub URL accessible by 
    Dataproc when JupyterHub runs on an AI Notebooks which by default would
    return a local address otherwise.
    """
    env = super().get_env()

    # Sets in the jupyterhub_config related to ai notebook.
    if 'NEW_JUPYTERHUB_API_URL' in env:
      env['JUPYTERHUB_API_URL'] = env['NEW_JUPYTERHUB_API_URL']
      env['JUPYTERHUB_ACTIVITY_URL'] = url_path_join(
          env['NEW_JUPYTERHUB_API_URL'],
          'users',
          # tolerate mocks defining only user.name
          getattr(self.user, 'escaped_name', self.user.name),
          'activity',
      )

    self.log.info(f'env is {env}')
    return env

################################################################################
# Custom Functions
################################################################################
  def getDataprocMasterFQDN(self):
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
      #return f'{self.clustername()}-m.c.{domain_project}.{domain_name}.internal'
      return f'{self.clustername()}-m.{self.zone}.c.{domain_project}.{domain_name}.internal'
    else:
      #return f'{self.clustername()}-m.c.{self.project}.internal'
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
    file_path = file_path.replace('gs://', '').split('/')
    bn = file_path[0]
    fp = "/".join(file_path[1:])
    
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
    default_path = self.default_notebooks_gcs_path
    user_folder = self.gcs_user_folder
    if not default_path or not user_folder:
      self.log.debug("Nothing to copy")
      return
    storage_client = storage.Client(project=self.project)
    bucket_name, folder_name = self._split_gcs_path(default_path)
    destination_bucket_name, destination_folder_name = self._split_gcs_path(user_folder)
    destination_folder_name += self.default_notebooks_folder
    self.log.debug(f'''Copy from {bucket_name}/{folder_name} to 
        {destination_bucket_name}/{destination_folder_name}''')
    
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
    # Manually set PATH for testing purposes
    self.temp_env = self.get_env()
    self.temp_env["PATH"] = "/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    self.env_str = json.dumps(self.temp_env)
    self.args_str = ' '.join(self.get_args())

    # Even if an admin provides a definition, ensures it is properly formed.
    self.cluster_definition = self._build_cluster_config(self.cluster_data)

    cluster = await self._create_cluster(self.cluster_definition)

    return cluster

  async def _create_cluster(self, cluster_data, try_count=3):
    """ Method implements custom retry functionality in order to change zone before each recall """
    while True:
      try:
        return self.dataproc_client.create_cluster(self.project, self.region, cluster_data)
      except (exceptions.PermissionDenied, exceptions.TooManyRequests, exceptions.ResourceExhausted) as e:

        if await self.get_cluster_status(self.clustername()) == ClusterStatus.State.DELETING:
          self.log.warning(f'Cluster {self.clustername()} pending deletion.')

        try_count -= 1
        if try_count > 0:
          cluster_data = self.change_zone_in_cluster_cfg(cluster_data)
          time.sleep(3)
          continue
        raise e

  def change_zone_in_cluster_cfg(self, cluster_data):
    current_zone_tmp = cluster_data['config']['gce_cluster_config']['zone_uri'].split('/')[-1]
    current_region = current_zone_tmp[0:-2]
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

    cluster_data['config']['gce_cluster_config']['zone_uri'] = f'https://www.googleapis.com/' \
                                                              f'compute/v1/projects/' \
                                                              f'{self.project}/zones/' \
                                                              f'{new_zone}'
    return cluster_data

  async def get_cluster_status(self, clustername):
    cluster = await self.get_cluster(clustername)
    if cluster is not None:
      return cluster.status.state
    return None
  
  async def exists(self, clustername):
    return (await self.get_cluster(clustername)) is not None

  async def get_cluster(self, clustername):
    try:
      return self.dataproc_client.get_cluster(self.project, self.region, 
                                              clustername)
    except exceptions.NotFound:
      return None

################################################################################
# Helper Functions
################################################################################

  def get_username(self, raw=False):
    return self.user.name if raw else re.sub(r'[^a-zA-Z0-9=]', '-', str(self.user.name))

  def clustername(self, cluster_name=None):
    """ JupyterHub provides a notebook per user, so the username is used to 
    distinguish between clusters. """
    if cluster_name is None:
      return f'dataprochub-{self.get_username()}'
    return cluster_name
  
  def calculate_config_value(self, key, path, default):
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
    return True if self.idle_checker and \
                   self.idle_checker.get('idle_job_path') and \
                   self.idle_checker.get('idle_path') else False

  def _split_gcs_path(self, path: str):
    gcs_prefix = "gs://"
    if path.startswith(gcs_prefix):
        path = path[len(gcs_prefix):]
    path = path.split("/")
    bucket = path[0]
    folder = "/".join(path[1:])
    if not folder.endswith("/"):
        folder += "/"
    return bucket, folder
  
  def convert_string_to_duration(self, data):
    """ A cluster export exports times as string using the JSON API but creating
    but a cluster uses Duration protobuf. This function checks if the fields 
    known to be affected by this behavior are present in the cluster config. 
    If so, it changes their value from string to a Duration in YAML. Duration 
    looks like {'seconds': 15, 'nanos': 0}. """
    self.log.info('Converting durations for {data}')

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
    if data['config'].setdefault("initialization_actions", []):
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
    
    self.log.info('Converted durations are in {data}')

    return data.copy()
  
  def _check_uri_geo(self, uri, uri_geo_slice, expected_geo, trim_zone=False):
    uri_geo = None
    uri_data = uri.split('/')
    if len(uri_data) > 1:
      uri_geo = uri_data[uri_geo_slice]
      if trim_zone:
        uri_geo = uri_geo[:-2]
      if uri_geo not in [expected_geo, 'global']:
        raise RuntimeError(f'''The location {uri_geo} of the uri {uri} in
            the yaml file does not match the Dataproc Hub's one {expected_geo}.
            Please, contact your admnistrator. ''')
    return uri_geo or uri




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
        i = i.replace("\n", "")
        if not is_exists:
            if i == "ID":
                is_exists = True
            else:
                break
        else:
            res.append(i)
    self.log.debug(f'Available autoscaling policies res')
    return res

  def _apply_users_configs(self, cluster_data):

    config = cluster_data['config']
    config.setdefault("initialization_actions", [])

    if self.user_options.get('pip_packages'):
      config.setdefault("gce_cluster_config", {})
      config['gce_cluster_config'].setdefault("metadata", {})
      config['gce_cluster_config']['metadata'].setdefault("PIP_PACKAGES", "")
      pip_packages = set(filter(None, set(
          config['gce_cluster_config']['metadata']['PIP_PACKAGES'].split(" ")
          + self.user_options.get('pip_packages').split(" "))))
      config['gce_cluster_config']['metadata']['PIP_PACKAGES'] = " ".join(pip_packages)
      config['initialization_actions'].append(
          {
              "executable_file": "gs://dataproc-initialization-actions/python/pip-install.sh"
          }
      )

    if self.user_options.get('condo_packages'):
      config.setdefault("gce_cluster_config", {})
      config['gce_cluster_config'].setdefault("metadata", {})
      config['gce_cluster_config']['metadata'].setdefault("CONDA_PACKAGES", "")
      conda_packages = set(filter(None, set(
          config['gce_cluster_config']['metadata']['CONDA_PACKAGES'].split(" ")
          + self.user_options.get('condo_packages').split(" "))))
      config['gce_cluster_config']['metadata']['CONDA_PACKAGES'] = " ".join(conda_packages)
      config['initialization_actions'].append(
          {
              "executable_file": "gs://dataproc-initialization-actions/python/conda-install.sh"
          }
      )

    if self.user_options.get('master_node_type'):
      config.setdefault("master_config", {})
      config['master_config']['machine_type_uri'] = self.user_options.get('master_node_type')

    if self.user_options.get('worker_node_type'):
      config.setdefault("worker_config", {})
      config['worker_config']['machine_type_uri'] = self.user_options.get('worker_node_type')

    if self.user_options.get('master_node_disc_size'):
      try:
        val = int(self.user_options.get('master_node_disc_size'))
        if val < 15:
          val = 15
        config.setdefault("master_config", {})
        config['master_config'].setdefault("disk_config", {})
        config['master_config']['disk_config']['boot_disk_size_gb'] = val
      except:
        pass

    if self.user_options.get('worker_node_disc_size'):
      try:
        val = int(self.user_options.get('worker_node_disc_size'))
        if val < 15:
          val = 15
        config.setdefault("worker_config", {})
        config['worker_config'].setdefault("disk_config", {})
        config['worker_config']['disk_config']['boot_disk_size_gb'] = val
      except:
        pass

    if self.user_options.get('worker_node_amount'):
      try:
        val = int(self.user_options.get('worker_node_amount'))
        if val < 2:
          val = 2
        config.setdefault("worker_config", {})
        config['worker_config']['num_instances'] = val
      except:
        pass

    autoscaling_policy = self.user_options.get('autoscaling_policy', '')
    if autoscaling_policy:
      cluster_data['config']['autoscaling_config'] = {
        "policy_uri": (
              f'''https://www.googleapis.com/compute/v1/projects/'''
              f'''{self.project}/locations/{self.region}/'''
              f'''autoscalingPolicies/{autoscaling_policy}''')
      }

    if self._is_custom_hive_settings():
        config['software_config']['properties']['hive:hive.metastore.schema.verification'] = 'false'
        config['software_config']['properties']['hive:javax.jdo.option.ConnectionURL'] = \
            f"jdbc:mysql://{self.user_options['hive_host']}/{self.user_options['hive_db']}"
        config['software_config']['properties']['hive:javax.jdo.option.ConnectionUserName'] = \
            self.user_options['hive_user']
        config['software_config']['properties']['hive:javax.jdo.option.ConnectionPassword'] = \
            self.user_options['hive_passwd']

    # To handle custom Java & Scala packages, use the following code to get values:
    # self.user_options.get('java_packages', '')
    # self.user_options.get('scala_packages', '')

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

    # Sets the cluster definition with form data.
    if self.user_options:
      metadata = {}
    
      # Reads values chosen by the user in the form and overwrites any existing
      # ones if relevant.
      gcs_config_file = self.user_options['cluster_type']
      cluster_zone = self.user_options.get('cluster_zone')

      # Reads the cluster config from yaml
      self.log.info(f'Reading config file at {gcs_config_file}')
      cluster_data = self.get_cluster_definition(gcs_config_file)

      # Defines default values if some key is not exists
      cluster_data['config'].setdefault("gce_cluster_config", {})
      cluster_data['config'].setdefault("master_config", {})
      cluster_data['config'].setdefault("initialization_actions", [])
      cluster_data['config'].setdefault("software_config", {})
      cluster_data['config']['software_config'].setdefault("properties", {})

      if 'metadata' in cluster_data['config']['gce_cluster_config']:
        metadata = cluster_data['config']['gce_cluster_config']['metadata']

      # Sets default network for the cluster if not already provided in YAML.
      if 'subnetwork_uri' not in cluster_data['config']['gce_cluster_config']:
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
              "https://www.googleapis.com/auth/cloud-platform"]

      init_actions = []
      if self._is_idle_checker_enable():
          init_actions.append(
              {
                  "executable_file": self.idle_checker.get('idle_job_path'),
                  'execution_timeout': {'seconds': 1800, 'nanos': 0}
              }
          )
          idle_path = self.idle_checker.get('idle_path')
          if idle_path.endswith("isIdle.sh"):
              idle_path = idle_path[:-len("isIdle.sh")]
          if idle_path.endswith("/"):
              idle_path = idle_path[:-len("/")]
          metadata["script_storage_location"] = idle_path
          metadata["max-idle"] = self.idle_checker.get('timeout', '60m')

      cluster_data['config']['gce_cluster_config']['metadata'] = metadata
      cluster_data['config']['initialization_actions'] = (
              init_actions + cluster_data['config']['initialization_actions']
      )

      if self.allow_custom_clusters and self.user_options.get('custom_cluster'):
        cluster_data = self._apply_users_configs(cluster_data)

      # Apply label to tag which host environment this cluster was spawned from
      cluster_data.setdefault('labels', {})
      cluster_data['labels']['goog-dataproc-notebook-spawner'] = (
          self.spawner_host_type.lower() if self.spawner_host_type != "" else "unknown"
      )
    
    # Always override project id and name
    cluster_data["project_id"] = self.project
    cluster_data["cluster_name"] = self.clustername()
    cluster_data.setdefault("config", {})
    
    # Sets the zone. Which one overwrites is decided in the form logic.
    cluster_data['config'].setdefault('gce_cluster_config', {})
    cluster_data['config']['gce_cluster_config']['zone_uri'] = (
        f'''https://www.googleapis.com/compute/v1/projects/{self.project}/'''
        f'''zones/{cluster_zone}''')
    
    # Overwrites some existing data with required values.
    cluster_data['config'].setdefault('software_config', {})
    cluster_data['config']['software_config'].setdefault('properties', {})
    
    (cluster_data['config']['software_config']['properties']
    ['dataproc:jupyter.hub.args']) = self.args_str
    (cluster_data['config']['software_config']['properties']
    ['dataproc:jupyter.hub.env']) = self.env_str
    (cluster_data['config']['software_config']['properties']
    ['dataproc:jupyter.hub.enabled']) = 'true'
    if self.gcs_user_folder:
      (cluster_data['config']['software_config']['properties']
      ['dataproc:jupyter.notebook.gcs.dir']) = self.gcs_user_folder
    
    if 'image_version' not in cluster_data['config']['software_config']:
      cluster_data['config']['software_config']['image_version'] = '1.4.16-debian9'

    if self.force_add_jupyter_component:
      cluster_data['config']['software_config'].setdefault('optional_components', [])
      optional_components = cluster_data['config']['software_config']['optional_components']
      if 'JUPYTER' not in optional_components:
        optional_components.append('JUPYTER')
      if 'ANACONDA' not in optional_components:
        optional_components.append('ANACONDA')
    
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
              cluster_data['config'][server_group]['machine_type_uri'].split('/')[-1])
        # Accelerator types must be in the same zone as the Dataproc Cluster.
        if 'accelerators' in cluster_data['config'][server_group]:
          for acc_idx, acc_val in enumerate(cluster_data['config'][server_group]
              ['accelerators']):
            (cluster_data['config'][server_group]['accelerators'][acc_idx]
                ['accelerator_type_uri']) = (acc_val['accelerator_type_uri'].split('/')[-1])
       
    # Temporarily disable Component Gateway until handled by core product.
    # TODO(mayran): Remove when code in prod.
    if (cluster_data['config']
        .setdefault('endpoint_config', {})
        .setdefault('enable_http_port_access', False)):
      cluster_data['config']['endpoint_config']['enable_http_port_access'] = False
    
    # Temporarily disable setting preemptibility field until Dataproc client
    # libraries support the enum value
    if (cluster_data['config'].setdefault('master_config', {})):
      cluster_data['config']['master_config'].pop('preemptibility', None)
    if (cluster_data['config'].setdefault('worker_config', {})):
      cluster_data['config']['worker_config'].pop('preemptibility', None)
    if (cluster_data['config'].setdefault('secondary_worker_config', {})):
      cluster_data['config']['secondary_worker_config'].pop('preemptibility', None)

    # Strip cluster-specific namenode properties
    if (cluster_data['config'].setdefault('software_config', {}) and
        cluster_data['config']['software_config'].setdefault('properties', {})):
        cluster_data['config']['software_config']['properties'].pop('hdfs:dfs.namenode.lifeline.rpc-address', None)
        cluster_data['config']['software_config']['properties'].pop('hdfs:dfs.namenode.servicerpc-address', None)

    self.log.info(f'Cluster configuration data is {cluster_data}')
    return cluster_data
