# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
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

from google.cloud import secretmanager_v1beta1 as secretmanager

def is_true(boolstring: str):
  """ Converts an environment variables to a Python boolean. """
  if boolstring.lower() in ('true', '1'):
    return True
  return False

# Listens on all interfaces.
c.JupyterHub.hub_ip = '0.0.0.0'

# Hostname that Cloud Dataproc can access to connect to the Hub.
c.JupyterHub.hub_connect_ip = socket.gethostbyname(socket.gethostname())

# Template for the user form.
c.JupyterHub.template_paths = ['/etc/jupyterhub/templates']

# Opens on JupyterLab instead of Jupyter's tree
c.Spawner.default_url = os.environ.get('SPAWNER_DEFAULT_URL', '/lab')

# The port that the spawned notebook listens on for the hub to connect
c.Spawner.port = 12345

print(os.environ)

# JupyterHub (Port must be 8080 to meet Inverting Proxy requirements.)
c.JupyterHub.spawner_class = 'dataprocspawner.DataprocSpawner'
c.JupyterHub.proxy_class = 'redirect-proxy'
c.JupyterHub.port = 8080
c.JupyterHub.allow_named_servers = is_true(os.environ.get('HUB_ALLOW_NAMED_SERVERS', ''))

# Authenticator
from gcpproxiesauthenticator.gcpproxiesauthenticator import GCPProxiesAuthenticator
c.JupyterHub.authenticator_class = GCPProxiesAuthenticator
c.GCPProxiesAuthenticator.check_header = 'X-Inverting-Proxy-User-Id'
c.GCPProxiesAuthenticator.template_to_render = 'welcome.html'

# Spawner
c.DataprocSpawner.project = os.environ.get('PROJECT', '')
c.DataprocSpawner.dataproc_configs = os.environ.get('DATAPROC_CONFIGS', '')
c.DataprocSpawner.region = os.environ.get('JUPYTERHUB_REGION', '')
c.DataprocSpawner.dataproc_default_subnet = os.environ.get('DATAPROC_DEFAULT_SUBNET', '')
c.DataprocSpawner.dataproc_service_account = os.environ.get('DATAPROC_SERVICE_ACCOUNT', '')
c.DataprocSpawner.dataproc_locations_list = os.environ.get('DATAPROC_LOCATIONS_LIST', '')
c.DataprocSpawner.machine_types_list = os.environ.get('DATAPROC_MACHINE_TYPES_LIST', '')
c.DataprocSpawner.cluster_name_pattern = os.environ.get('CLUSTER_NAME_PATTERN', 'dataprochub-{}')
c.DataprocSpawner.allow_custom_clusters = is_true(os.environ.get('DATAPROC_ALLOW_CUSTOM_CLUSTERS', ''))
c.DataprocSpawner.allow_random_cluster_names = is_true(os.environ.get('ALLOW_RANDOM_CLUSTER_NAMES', ''))
c.DataprocSpawner.show_spawned_clusters_in_notebooks_list = is_true(os.environ.get('SHOW_SPAWNED_CLUSTERS', ''))
c.DataprocSpawner.force_single_user = is_true(os.environ.get('FORCE_SINGLE_USER', ''))
c.DataprocSpawner.gcs_notebooks = os.environ.get('GCS_NOTEBOOKS', '')
if not c.DataprocSpawner.gcs_notebooks:
  c.DataprocSpawner.gcs_notebooks = os.environ.get('NOTEBOOKS_LOCATION', '')
c.DataprocSpawner.default_notebooks_gcs_path = os.environ.get('GCS_EXAMPLES_PATH', '')
if not c.DataprocSpawner.default_notebooks_gcs_path:
  c.DataprocSpawner.default_notebooks_gcs_path = os.environ.get('NOTEBOOKS_EXAMPLES_LOCATION', '')

admins = os.environ.get('ADMINS', '')
if admins:
  c.Authenticator.admin_users = admins.split(',')

# # Idle checker https://github.com/blakedubois/dataproc-idle-check
idle_job_path = os.environ.get('IDLE_JOB_PATH', '')
idle_path = os.environ.get('IDLE_PATH', '')
idle_timeout = os.environ.get('IDLE_TIMEOUT', '1d')

if (idle_job_path and idle_path):
  c.DataprocSpawner.idle_checker = {
    'idle_job_path': idle_job_path,  # gcs path to https://github.com/blakedubois/dataproc-idle-check/blob/master/isIdleJob.sh
    'idle_path': idle_path,          # gcs path to https://github.com/blakedubois/dataproc-idle-check/blob/master/isIdle.sh
    'timeout': idle_timeout          # idle time after which cluster will be shutdown
  }

## End of common setup ##
