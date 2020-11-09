import os
import requests
import socket

from tornado import web

from google.cloud import secretmanager_v1beta1 as secretmanager

def access_secret_version(project_id, secret_id, version_id):
  """
  Accesses the payload for the given secret version if one exists. The version
  can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
  """
  client = secretmanager.SecretManagerServiceClient()
  name = client.secret_version_path(project_id, secret_id, version_id)
  response = client.access_secret_version(name)
  payload = response.payload.data.decode('UTF-8')
  return payload


def is_true(boolstring: str):
  """ Converts an environment variables to a Python boolean. """
  if boolstring.lower() in ("true", "1"):
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

# Spawner
c.JupyterHub.spawner_class = 'dataprocspawner.DataprocSpawner'
c.DataprocSpawner.project = os.environ.get('PROJECT', '')
c.DataprocSpawner.dataproc_configs = os.environ.get('DATAPROC_CONFIGS', '')
c.DataprocSpawner.region = os.environ.get('JUPYTERHUB_REGION', '')
c.DataprocSpawner.dataproc_default_subnet = os.environ.get('DATAPROC_DEFAULT_SUBNET', '')
c.DataprocSpawner.dataproc_service_account = os.environ.get('DATAPROC_SERVICE_ACCOUNT', '')
c.DataprocSpawner.dataproc_locations_list = os.environ.get('DATAPROC_LOCATIONS_LIST', '')
c.DataprocSpawner.machine_types_list = os.environ.get('DATAPROC_MACHINE_TYPES_LIST', '')
c.DataprocSpawner.cluster_name_pattern = os.environ.get('CLUSTER_NAME_PATTERN', 'dataprochub-{}')
c.DataprocSpawner.allow_custom_clusters = is_true(os.environ.get('DATAPROC_ALLOW_CUSTOM_CLUSTERS', ''))
# Support multiple environment variable names for the GCS notebooks path
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
    "idle_job_path": idle_job_path,  # gcs path to https://github.com/blakedubois/dataproc-idle-check/blob/master/isIdleJob.sh
    "idle_path": idle_path,          # gcs path to https://github.com/blakedubois/dataproc-idle-check/blob/master/isIdle.sh
    "timeout": idle_timeout          # idle time after which cluster will be shutdown
  }

## End of common setup ##
