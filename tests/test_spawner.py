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
from google.cloud import dataproc_v1beta2
from google.cloud.dataproc_v1beta2.proto import clusters_pb2
from google.longrunning import operations_pb2
from google.cloud import storage
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

  @pytest.mark.asyncio
  async def test_start_normal(self):
    operation = operations_pb2.Operation()

    # Mock the Dataproc API client
    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_client.create_cluster.return_value = operation

    # Force no existing clusters to bypass the check in the spawner
    mock_client.get_cluster.return_value = None

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

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

    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "test-create-existing"
    assert spawner.project == "test-create-existing"

    (ip, port) = await spawner.start()
    assert ip == f'dataprochub-fake-m.{self.zone}.c.{spawner.project}.internal'
    assert port == 0

    mock_client.create_cluster.assert_not_called()

  @pytest.mark.asyncio
  async def test_stop_normal(self):

    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "test-stop"
    assert spawner.project == "test-stop"
    assert spawner.region == self.region

    response = await spawner.stop()

    mock_client.delete_cluster.assert_called_once_with("test-stop", self.region, 'dataprochub-fake')

  @pytest.mark.asyncio
  async def test_stop_no_cluster(self):

    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_client.get_cluster.return_value = None

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "test-stop-no-cluster"
    assert spawner.project == "test-stop-no-cluster"

    response = await spawner.stop()

    mock_client.delete_cluster.assert_not_called()

  @pytest.mark.asyncio
  async def test_poll_normal(self):

    expected_response = {
      "status": {
          "state": "RUNNING"
      }
    }
    expected_response = clusters_pb2.Cluster(**expected_response)

    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_client.get_cluster.return_value = expected_response

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "test-poll"
    assert spawner.project == "test-poll"

    assert await spawner.poll() == None

  @pytest.mark.asyncio
  async def test_poll_create(self):

    expected_response = {
      "status": {
          "state": "CREATING"
      }
    }
    expected_response = clusters_pb2.Cluster(**expected_response)

    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_client.get_cluster.return_value = expected_response

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "test-poll-create"
    assert spawner.project == "test-poll-create"

    assert await spawner.poll() == None

  @pytest.mark.asyncio
  async def test_poll_no_cluster(self):

    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_client.get_cluster.return_value = None

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "test-poll-no-cluster"
    assert spawner.project == "test-poll-no-cluster"

    assert await spawner.poll() == 1

  @pytest.mark.asyncio
  async def test_normal_zonal_dns(self):
    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "non-domain-scoped"
    assert spawner.project == "non-domain-scoped"

    (ip, port) = await spawner.start()
    assert ip == f'dataprochub-fake-m.{self.zone}.c.{spawner.project}.internal'
    assert port == 0

  @pytest.mark.asyncio
  async def test_domain_scoped_zonal_dns(self):
    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "test:domain-scoped"
    assert spawner.project == "test:domain-scoped"

    (ip, port) = await spawner.start()
    assert ip == f'dataprochub-fake-m.{self.zone}.c.domain-scoped.test.internal'
    assert port == 0

  # YAML files
  # Tests Dataproc cluster configurations.
  
  def test_cluster_definition_is_core_elements(self, monkeypatch):
    """ Some keys must always be present for JupyterHub to work. """
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/core_elements.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    mock_dataproc_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_gcs_client = mock.create_autospec(storage.Client())
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True)
       
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

    assert 'ANACONDA' in config_built['config']['software_config']['optional_components']
    assert 'JUPYTER' in config_built['config']['software_config']['optional_components']

    assert 'dataproc:jupyter.hub.args' in config_built['config']['software_config']['properties']
    assert 'dataproc:jupyter.hub.enabled' in config_built['config']['software_config']['properties']
    #assert 'dataproc:jupyter.notebook.gcs.dir' in config_built['config']['software_config']['properties']
    assert 'dataproc:jupyter.hub.env' in config_built['config']['software_config']['properties']
  
  def test_cluster_definition_check_core_fields(self, monkeypatch):
    """ Values chosen by the user through the form overwrites others. If the 
    admin wants to prevent that behavior, they should remove form elements. 
    TODO(mayran): Check keys so users can not add custom ones. """
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/basic.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    mock_dataproc_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_gcs_client = mock.create_autospec(storage.Client())
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True)
       
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.zone = "test-self1-b"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['project_id'] == 'test-project'
    assert config_built['cluster_name'] == 'test-clustername'

  
  def test_cluster_definition_keep_core_values(self, monkeypatch):
    """ Some system's default values must remain no matter what. """
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/rich.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    mock_dataproc_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_gcs_client = mock.create_autospec(storage.Client())
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True)
       
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.zone = "test-self1-b"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'rich.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['project_id'] == 'test-project'
    assert config_built['cluster_name'] == 'test-clustername'

    assert config_built['config']['software_config']['properties']['dataproc:jupyter.hub.args'] == 'test-args-str'
    assert config_built['config']['software_config']['properties']['dataproc:jupyter.hub.enabled'] == 'true'
    assert config_built['config']['software_config']['properties']['dataproc:jupyter.notebook.gcs.dir'] == ''
    assert config_built['config']['software_config']['properties']['dataproc:jupyter.hub.env'] == 'test-env-str'

  def test_cluster_definition_does_form_overwrite(self, monkeypatch):
    """ Values chosen by the user through the form overwrites others. If the 
    admin wants to prevent that behavior, they should remove form elements. 
    TODO(mayran): Check keys so users can not add custom ones. """
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/rich.yaml', 'r').read()
      return config_string
    
    mock_dataproc_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_gcs_client = mock.create_autospec(storage.Client())
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True)
       
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)

    spawner.project = "test-project"
    spawner.zone = "test-self1-b"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'rich.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['config']['gce_cluster_config']['zone_uri'].split('/')[-1] == 'test-form1-a'

  def test_camel_case(self, monkeypatch):
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/export.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    mock_dataproc_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_gcs_client = mock.create_autospec(storage.Client())
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True)
        
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.zone = "test-self1-b"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert (config_built['config']['worker_config']['machine_type_uri'] == 
        "https://www.googleapis.com/compute/v1/projects/alluxio-demo/zones/us-east1-d/machineTypes/n1-highmem-16")
    
  
  def test_duration(self, monkeypatch):
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/export.yaml', 'r').read()
      return config_string
    
    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    mock_dataproc_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())
    mock_gcs_client = mock.create_autospec(storage.Client())
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client, user=MockUser(), _mock=True)
        
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.project = "test-project"
    spawner.zone = "test-self1-b"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['config']['initialization_actions'][0]['execution_timeout']['seconds'] == 600
        

