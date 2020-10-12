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

""" Unit tests for DataprocSpawner.

Unit tests for methods within DataprocSpawner (start, stop, and poll).
"""
from collections import namedtuple
from dataprocspawner import DataprocSpawner
from google.auth.credentials import AnonymousCredentials
from google.cloud.dataproc_v1beta2 import ClusterControllerClient
from google.cloud.dataproc_v1beta2 import Cluster
from google.cloud.dataproc_v1beta2 import ClusterStatus
from google.cloud.dataproc_v1beta2.types.shared import Component
from google.longrunning import operations_pb2
from google.cloud import storage
from google.cloud.storage.blob import Blob
from jupyterhub.objects import Hub, Server
from unittest import mock
import json
import pytest

class MockUser(mock.Mock):
  name = 'fake'
  server = Server()

  @property
  def escaped_name(self):
      return self.name

  @property
  def url(self):
      return self.server.url

class TestDataprocSpawner:

  # Spawner defaults to us-central1 for the region
  region = "us-central1"
  zone = "us-central1-a"
  gcs_notebooks = "gs://users-notebooks"

  @pytest.mark.asyncio
  async def test_start_normal(self):
    operation = operations_pb2.Operation()

    # Mock the Dataproc API client
    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.create_cluster.return_value = operation

    # Force no existing clusters to bypass the check in the spawner
    mock_client.get_cluster.return_value = None

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    # Test that the traitlets work
    spawner.project = "test-create"
    assert spawner.project == "test-create"
    assert spawner.region == self.region

    (ip, port) = await spawner.start()
    assert ip == f'dataprochub-fake-m.{self.zone}.c.{spawner.project}.internal'
    # JupyterHub defaults to 0 if no port set
    assert port == 0

    mock_client.create_cluster.assert_called_once()

    assert spawner.cluster_definition['cluster_name'] == 'dataprochub-fake'
    assert spawner.cluster_definition['config']['gce_cluster_config']['zone_uri'] == f'https://www.googleapis.com/compute/v1/projects/{spawner.project}/zones/{spawner.zone}'

    env = json.loads(spawner.cluster_definition['config']['software_config']['properties']['dataproc:jupyter.hub.env'])
    assert env['JUPYTERHUB_API_URL'] is not None

  @pytest.mark.asyncio
  async def test_start_existing_clustername(self):

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    spawner.project = "test-create-existing"
    assert spawner.project == "test-create-existing"

    (ip, port) = await spawner.start()
    assert ip == f'dataprochub-fake-m.{self.zone}.c.{spawner.project}.internal'
    assert port == 0

    mock_client.create_cluster.assert_not_called()

  @pytest.mark.asyncio
  async def test_stop_normal(self):

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    spawner.project = "test-stop"
    assert spawner.project == "test-stop"
    assert spawner.region == self.region

    response = await spawner.stop()

    mock_client.delete_cluster.assert_called_once_with(
        project_id="test-stop",
        region=self.region,
        cluster_name='dataprochub-fake')

  @pytest.mark.asyncio
  async def test_stop_no_cluster(self):

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.get_cluster.return_value = None

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    spawner.project = "test-stop-no-cluster"
    assert spawner.project == "test-stop-no-cluster"

    response = await spawner.stop()

    mock_client.delete_cluster.assert_not_called()

  @pytest.mark.asyncio
  async def test_poll_normal(self):

    expected_response = {
      "status": {
          "state": ClusterStatus.State.RUNNING
      }
    }
    expected_response = Cluster(**expected_response)

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.get_cluster.return_value = expected_response

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    spawner.project = "test-poll"
    assert spawner.project == "test-poll"

    assert await spawner.poll() == None

  @pytest.mark.asyncio
  async def test_poll_create(self):

    expected_response = {
      "status": {
          "state": ClusterStatus.State.CREATING
      }
    }
    expected_response = Cluster(**expected_response)

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.get_cluster.return_value = expected_response

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    spawner.project = "test-poll-create"
    assert spawner.project == "test-poll-create"

    assert await spawner.poll() == None

  @pytest.mark.asyncio
  async def test_poll_no_cluster(self):

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.get_cluster.return_value = None

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    spawner.project = "test-poll-no-cluster"
    assert spawner.project == "test-poll-no-cluster"

    assert await spawner.poll() == 1

  @pytest.mark.asyncio
  async def test_normal_zonal_dns(self):
    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    spawner.project = "non-domain-scoped"
    assert spawner.project == "non-domain-scoped"

    (ip, port) = await spawner.start()
    assert ip == f'dataprochub-fake-m.{self.zone}.c.{spawner.project}.internal'
    assert port == 0

  @pytest.mark.asyncio
  async def test_domain_scoped_zonal_dns(self):
    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    spawner.project = "test:domain-scoped"
    assert spawner.project == "test:domain-scoped"

    (ip, port) = await spawner.start()
    assert ip == f'dataprochub-fake-m.{self.zone}.c.domain-scoped.test.internal'
    assert port == 0

  # YAML files
  # Tests Dataproc cluster configurations.
  
  def test_clean_gcs_path(self, monkeypatch):
    path = "gs://bucket/config/"

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    assert spawner._clean_gcs_path(path) == "gs://bucket/config"
    assert spawner._clean_gcs_path(path, return_gs=False) == "bucket/config"
    assert spawner._clean_gcs_path(path, return_slash=True) == "gs://bucket/config/"
  
  def test_config_paths(self, monkeypatch):
    """ Checks that configuration paths are found. """

    config_hierarchy = [
      "bucket/listme/file_L1.yaml",
      "bucket/config/file_A1.yaml",
      "bucket/config/file_A2.yaml",
      "bucket/file_B1.yaml",
      "bucket-two/config/two/file_C1.yaml"
    ]

    expected = config_hierarchy

    def test_list_blobs(*args, **kwargs):
      """ Rewrites library function to reads a custom list of paths vs real GCS.
      https://googleapis.dev/python/storage/latest/_modules/google/cloud/storage/client.html#Client.list_blobs
      """
      bucket_or_name = args[0]
      prefix = kwargs['prefix']
      candidate_path = f'{bucket_or_name}/{prefix}'
      config_paths = []

      for c in config_hierarchy:
        if c.startswith(candidate_path):
          fn = '/'.join(c.split('/')[1:])
          b = Blob(bucket='dummy', name=fn)
          config_paths.append(b)

      return iter(config_paths)
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
       
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(mock_gcs_client, "list_blobs", test_list_blobs)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.zone = "test-self1-b"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.dataproc_configs = "gs://bucket/config/,bucket/config/file_A1.yaml,bucket/file_B1.yaml,bucket-notexist/file.yaml,bucket/file-notexist.yaml,bucket/listme/,bucket/config-notexist/file.yaml,gs://bucket/listme/,bucket/config,bucket-two,"

    read_paths = spawner._list_gcs_files(spawner.dataproc_configs)

    assert type(read_paths) == type(config_hierarchy)
    assert len(read_paths) == len(config_hierarchy)
    assert set(read_paths) == set(config_hierarchy)
  
  def test_minimium_cluster_definition(self, monkeypatch):
    """ Some keys must always be present for JupyterHub to work. """
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/minimum.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
       
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.zone = "test-self1-b"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"

    config_built = spawner._build_cluster_config()

    assert 'project_id' in config_built
    assert 'cluster_name' in config_built

    assert config_built['project_id'] == 'test-project'
    assert config_built['cluster_name'] == 'test-clustername'
    
    assert config_built['config']['gce_cluster_config']['zone_uri'].split('/')[-1] == 'test-self1-b'

    assert Component['JUPYTER'].value in config_built['config']['software_config']['optional_components']
    assert Component['ANACONDA'].value in config_built['config']['software_config']['optional_components']

    assert 'dataproc:jupyter.hub.args' in config_built['config']['software_config']['properties']
    assert 'dataproc:jupyter.hub.enabled' in config_built['config']['software_config']['properties']
    # assert 'dataproc:jupyter.notebook.gcs.dir' in config_built['config']['software_config']['properties']
    assert 'dataproc:jupyter.hub.env' in config_built['config']['software_config']['properties']
  
  def test_cluster_definition_check_core_fields(self, monkeypatch):
    """ Values chosen by the user through the form overwrites others. If the 
    admin wants to prevent that behavior, they should remove form elements. 
    TODO(mayran): Check keys so users can not add custom ones. """
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/basic.yaml', 'r').read()
      return config_string

    def test_username(*args, **kwargs):
      return 'foo-user'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
       
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "get_username", test_username)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.cluster_name_pattern = 'my-cluster-{}'
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['cluster_name'] == 'my-cluster-foo-user'
    assert config_built['project_id'] == 'test-project'
  
  def test_cluster_definition_keep_core_values(self, monkeypatch):
    """ Some system's default values must remain no matter what. """
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/basic.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
       
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['project_id'] == 'test-project'
    assert config_built['cluster_name'] == 'test-clustername'

    assert config_built['config']['software_config']['properties']['dataproc:jupyter.hub.args'] == 'test-args-str'
    assert config_built['config']['software_config']['properties']['dataproc:jupyter.hub.enabled'] == 'true'
    # assert config_built['config']['software_config']['properties']['dataproc:jupyter.notebook.gcs.dir'] == f'gs://users-notebooks/fake'
    assert config_built['config']['software_config']['properties']['dataproc:jupyter.hub.env'] == 'test-env-str'

  def test_cluster_definition_overrides(self, monkeypatch):
    """Check that config settings incompatible with JupyterHub are overwritten correctly."""
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/export.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
       
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'export.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    # Verify that we disable Component Gateway (temporarily)
    assert config_built['config']['endpoint_config']['enable_http_port_access'] == False
    # Verify that we disable preemptibility (temporarily)
    assert 'preemptibility' not in config_built['config']['master_config']
    assert 'preemptibility' not in config_built['config']['worker_config']
    assert 'preemptibility' not in config_built['config']['secondary_worker_config']
    # Verify that we removed cluster-specific namenode properties
    assert 'hdfs:dfs.namenode.lifeline.rpc-address' not in config_built['config']['software_config']['properties']
    assert 'hdfs:dfs.namenode.servicerpc-address' not in config_built['config']['software_config']['properties']

  def test_cluster_definition_does_form_overwrite(self, monkeypatch):
    """ Values chosen by the user through the form overwrites others. If the 
    admin wants to prevent that behavior, they should remove form elements. 
    TODO(mayran): Check keys so users can not add custom ones. """
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/basic.yaml', 'r').read()
      return config_string
    
    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
       
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['config']['gce_cluster_config']['zone_uri'].split('/')[-1] == 'test-form1-a'

  def test_camel_case(self, monkeypatch):
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/custom.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
        
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'custom.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    expected_dict = {
      'project_id': 'test-project',
      'labels': {'goog-dataproc-notebook-spawner': 'unknown'},
      'cluster_name': 'test-clustername',
      'config': {
        'autoscaling_config': {
          'policy_uri': 'projects/my-project/regions/us-east1/autoscalingPolicies/policy-abc123'},
        'config_bucket': 'bucket-dash',
        'endpoint_config': {'enable_http_port_access': True},
        'gce_cluster_config': {
          'metadata': {
            'KeyCamelCase': 'UlowUlow',
            'key_with_underscore': 'https://downloads.io/protected/files/enterprise-trial.tar.gz',
            'key_with_underscore_too': 'some_UPPER_and_UlowerU:1234'
          },
          'zone_uri': 'https://www.googleapis.com/compute/v1/projects/test-project/zones/test-form1-a'
        },
        'initialization_actions': [],
        'lifecycle_config': {},
        'master_config': {
          'machine_type_uri': 'machine.1.2_numbers',
          'min_cpu_platform': 'AUTOMATIC',
          'disk_config': {
            'boot_disk_size_gb': 1000,
            'key_something': 'https://all-sort.of/lowerUpper/including-Dash/disk.1.2_numbers',
          },
        },
        'secondary_worker_config': {},
        'worker_config': {},
        'software_config': {
          'image_version': '1.4-debian9',
          'optional_components': [
              Component.JUPYTER.value,
              Component.ANACONDA.value],
          'properties': {
            'dataproc:jupyter.hub.args': 'test-args-str',
            'dataproc:jupyter.hub.enabled': 'true',
            'dataproc:jupyter.hub.env': 'test-env-str',
            'dataproc:jupyter.notebook.gcs.dir': 'gs://users-notebooks/fake',
            'key-with-dash:UPPER_UPPER': '4000',
            'key-with-dash-too:UlowUlowUlow': '85196m',
            'key:and.multiple.dots.lowUlowUlow': '13312m'
          }
        }
      }
    }

    assert expected_dict == config_built    
  
  def test_duration(self, monkeypatch):
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/duration.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
        
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'duration.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    # Test 600s string
    assert config_built['config']['initialization_actions'][0]['execution_timeout']['seconds'] == 600
    # Test Duration protobuf
    assert config_built['config']['initialization_actions'][1]['execution_timeout']['seconds'] == 600
  
  def test_metadata(self, monkeypatch):
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/basic.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
        
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['config']['gce_cluster_config']['metadata'] == {
      'm1': 'v1',
      'm2': 'v2'
    }
  
  def test_uris(self, monkeypatch):
    """ Test that all official URI patterns work and geo location match."""
    import yaml

    def test_read_file_string(*args, **kwargs):
      config_string = open('./tests/test_data/basic.yaml', 'r').read()
      return config_string
    
    def test_read_file_uri(*args, **kwargs):
      config_string = open('./tests/test_data/basic_uri.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)
        
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file_string)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['config']['gce_cluster_config']['subnetwork_uri'] == "default"

    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file_uri)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['config']['gce_cluster_config']['subnetwork_uri'] == "projects/test-project/regions/us-east1/subnetworks/default"


  def test_locations(self, monkeypatch):
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/basic_uri.yaml', 'r').read()
      return config_string

    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic_uri.yaml',
      'cluster_zone': 'us-east1-d'
    }

    user_zone = spawner.user_options['cluster_zone']
    user_region = user_zone[:-2]

    config_built = spawner._build_cluster_config()

    assert config_built['config']['gce_cluster_config']['subnetwork_uri'].split('/')[-3] == user_region
    assert config_built['config']['master_config']['machine_type_uri'] == 'n1-standard-4'
    assert config_built['config']['worker_config']['machine_type_uri'] == 'n1-highmem-16'
    assert config_built['config']['secondary_worker_config']['machine_type_uri'] == 'n1-standard-4'
    assert config_built['config']['master_config']['accelerators'][0]['accelerator_type_uri'] == 'nvidia-tesla-v100'

  def test_image_version_supports_component_gateway(self, monkeypatch):
    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks)

    assert spawner._validate_image_version_supports_component_gateway('1.3') is True
    assert spawner._validate_image_version_supports_component_gateway('1.3-debian9') is True
    assert spawner._validate_image_version_supports_component_gateway('1.3.6-debian9') is False
    assert spawner._validate_image_version_supports_component_gateway('1.3.59-debian9') is True
    assert spawner._validate_image_version_supports_component_gateway('1.3.999-debian9') is True
    assert spawner._validate_image_version_supports_component_gateway('1.4-debian10') is True
    assert spawner._validate_image_version_supports_component_gateway('1.4.6-debian10') is False
    assert spawner._validate_image_version_supports_component_gateway('1.4.31-debian10') is True
    assert spawner._validate_image_version_supports_component_gateway('1.5-debian10') is True
    assert spawner._validate_image_version_supports_component_gateway('1.5.0-debian10') is False
    assert spawner._validate_image_version_supports_component_gateway('1.5.5-debian10') is True
    assert spawner._validate_image_version_supports_component_gateway('2') is True
    assert spawner._validate_image_version_supports_component_gateway('2.0') is True
    assert spawner._validate_image_version_supports_component_gateway('2.0.0') is True
    assert spawner._validate_image_version_supports_component_gateway('2.3.0') is True
    assert spawner._validate_image_version_supports_component_gateway('2.0.0-RC1-preview') is True
    assert spawner._validate_image_version_supports_component_gateway('weird-unexpected-version-124.3.v2.2020-02-15') is True
    assert spawner._validate_image_version_supports_component_gateway('1.3.weird-version-again') is True
