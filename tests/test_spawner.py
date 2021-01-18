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
from unittest import mock
import json
import pytest
import math
import threading
import json
import pytest
import asyncio
import dataprocspawner
from dataprocspawner import DataprocSpawner
from google.auth.credentials import AnonymousCredentials
from google.cloud.dataproc_v1beta2 import (
  ClusterControllerClient, Cluster, ClusterStatus)
from google.cloud.dataproc_v1beta2.types.shared import Component
from google.longrunning import operations_pb2
from google.cloud import storage, logging_v2
from google.cloud.logging_v2.types import LogEntry
from google.cloud.storage.blob import Blob
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct
from googleapiclient import discovery
from jupyterhub.objects import Hub, Server
from unittest import mock
from types import SimpleNamespace

class MockOperation(object):
  def __init__(
      self, name, cluster_uuid, inner_state=None, timeout=2.0, op_done=False):
    status = SimpleNamespace(inner_state=inner_state)
    metadata = SimpleNamespace(cluster_uuid=cluster_uuid, status=status)
    operation = SimpleNamespace(name=name)
    self.name = name
    self.op_done = op_done
    self.metadata = metadata
    self.operation = operation
    self.timer = threading.Timer(timeout, self.set_delay_done)
    self.timer.start()

  def done(self):
    return self.op_done

  def set_delay_done(self):
    self.op_done = True

class MockUser(mock.Mock):
  name = 'fake@example.com'
  base_url = '/user/fake'
  server = Server()

  @property
  def escaped_name(self):
    return self.name

  @property
  def url(self):
    return self.server.url

class TestDataprocSpawner:

  # Spawner defaults to us-central1 for the region
  region = 'us-central1'
  zone = 'us-central1-a'
  gcs_notebooks = 'gs://users-notebooks'

  @pytest.mark.asyncio
  async def test_start_normal(self, monkeypatch):
    operation = operations_pb2.Operation()

    # Mock the Dataproc API client
    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.create_cluster.return_value = operation
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    # Force no existing clusters to bypass the check in the spawner
    mock_client.get_cluster.return_value = None

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-create')

    async def test_get_cluster_notebook_endpoint(*args, **kwargs):
      await asyncio.sleep(0)
      return f'https://abcd1234-dot-{self.region}.dataproc.googleusercontent.com/jupyter'

    monkeypatch.setattr(spawner, "get_cluster_notebook_endpoint", test_get_cluster_notebook_endpoint)

    # Test that the traitlets work
    assert spawner.project == 'test-create'
    assert spawner.region == self.region

    url = await spawner.start()
    assert url == f'https://abcd1234-dot-{self.region}.dataproc.googleusercontent.com/jupyter'

    mock_client.create_cluster.assert_called_once()

    assert spawner.cluster_definition['cluster_name'] == f'dataprochub-{spawner.get_username()}'
    assert (spawner.cluster_definition['config']['gce_cluster_config']['zone_uri']) == (
        f'https://www.googleapis.com/compute/v1/projects/{spawner.project}/zones/{spawner.zone}')

    env = json.loads(spawner.cluster_definition['config']['software_config']
        ['properties']['dataproc:jupyter.hub.env'])
    assert env['JUPYTERHUB_API_URL'] is not None

  @pytest.mark.asyncio
  async def test_named_server(self, monkeypatch):
    operation = operations_pb2.Operation()

    # Mock the Dataproc API client
    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.create_cluster.return_value = operation
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    # Force no existing clusters to bypass the check in the spawner
    mock_client.get_cluster.return_value = None

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-create')

    class _SubSpawner(DataprocSpawner):
      name = 'server1'
    spawner.__class__ = _SubSpawner

    async def test_get_cluster_notebook_endpoint(*args, **kwargs):
      await asyncio.sleep(0)
      return f'https://abcd1234-dot-{self.region}.dataproc.googleusercontent.com/jupyter'

    monkeypatch.setattr(spawner, "get_cluster_notebook_endpoint", test_get_cluster_notebook_endpoint)

    url = await spawner.start()
    mock_client.create_cluster.assert_called_once()

    assert spawner.cluster_definition['cluster_name'] == f'dataprochub-{spawner.get_username()}-server1'

  @pytest.mark.asyncio
  async def test_start_existing_clustername(self, monkeypatch):

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-create-existing')

    async def test_get_cluster_notebook_endpoint(*args, **kwargs):
      await asyncio.sleep(0)
      return f'https://abcd1234-dot-{self.region}.dataproc.googleusercontent.com/jupyter'

    monkeypatch.setattr(spawner, "get_cluster_notebook_endpoint", test_get_cluster_notebook_endpoint)

    assert spawner.project == "test-create-existing"

    url = await spawner.start()
    assert url == f'https://abcd1234-dot-{self.region}.dataproc.googleusercontent.com/jupyter'

    mock_client.create_cluster.assert_not_called()

  @pytest.mark.asyncio
  async def test_stop_normal(self):

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-stop')

    assert spawner.project == 'test-stop'
    assert spawner.region == self.region

    response = await spawner.stop()

    mock_client.delete_cluster.assert_called_once_with(
        project_id='test-stop',
        region=self.region,
        cluster_name=f'dataprochub-{spawner.get_username()}')

  @pytest.mark.asyncio
  async def test_stop_no_cluster(self):

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.get_cluster.return_value = None
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-stop-no-cluster')

    assert spawner.project == 'test-stop-no-cluster'

    response = await spawner.stop()

    mock_client.delete_cluster.assert_not_called()

  @pytest.mark.asyncio
  async def test_poll_normal(self):

    expected_response = {
      'status': {
          'state': ClusterStatus.State.RUNNING
      }
    }
    expected_response = Cluster(**expected_response)

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.get_cluster.return_value = expected_response
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-poll')

    assert spawner.project == 'test-poll'

    assert await spawner.poll() == None

  @pytest.mark.asyncio
  async def test_poll_create(self):

    expected_response = {
      'status': {
          'state': ClusterStatus.State.CREATING
      }
    }
    expected_response = Cluster(**expected_response)

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.get_cluster.return_value = expected_response
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-poll-create')

    assert spawner.project == 'test-poll-create'

    assert await spawner.poll() == None

  @pytest.mark.asyncio
  async def test_poll_no_cluster(self, monkeypatch):

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_client.get_cluster.return_value = None
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-poll-no-cluster')

    async def test_get_cluster_notebook_endpoint(*args, **kwargs):
      await asyncio.sleep(0)
      return f'https://abcd1234-dot-{self.region}.dataproc.googleusercontent.com/jupyter'

    monkeypatch.setattr(spawner, "get_cluster_notebook_endpoint", test_get_cluster_notebook_endpoint)

    assert spawner.project == 'test-poll-no-cluster'
    assert await spawner.poll() == 1

  # YAML files
  # Tests Dataproc cluster configurations.

  def test_clean_gcs_path(self, monkeypatch):
    path = "gs://bucket/config/"

    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-project')

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
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')

    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(mock_gcs_client, "list_blobs", test_list_blobs)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.zone = "test-self1-b"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.dataproc_configs = (
        "gs://bucket/config/,"
        "bucket/config/file_A1.yaml,"
        "bucket/file_B1.yaml,"
        "bucket-notexist/file.yaml,"
        "bucket/file-notexist.yaml,"
        "bucket/listme/,"
        "bucket/config-notexist/file.yaml,"
        "gs://bucket/listme/,bucket/config,bucket-two,")

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

    def test_read_file_preview(*args, **kwargs):
      config_string = open('./tests/test_data/minimum.yaml', 'r').read()
      config_string = config_string.replace('1.4.16', 'preview')
      return config_string

    def test_read_file_2_0(*args, **kwargs):
      config_string = open('./tests/test_data/minimum.yaml', 'r').read()
      return config_string.replace('1.4.16', '2.0.0-RC19')

    def test_read_file_2_0_with_anaconda(*args, **kwargs):
      config_string = open('./tests/test_data/minimum.yaml', 'r').read()
      config_string = config_string.replace('1.4.16', '2.0.0-RC19')
      config_string += "\n    optionalComponents:\n    - JUPYTER\n    - ANACONDA\n"
      return config_string

    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

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
    assert 'dataproc:jupyter.hub.env' in config_built['config']['software_config']['properties']

    spawner.user_options = {
      'cluster_type': 'minimum.yaml',
    }
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file_preview)

    config_built = spawner._build_cluster_config()
    assert Component['JUPYTER'].value in config_built['config']['software_config']['optional_components']
    assert Component['ANACONDA'].value not in config_built['config']['software_config']['optional_components']

    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file_2_0)

    config_built = spawner._build_cluster_config()
    assert Component['JUPYTER'].value in config_built['config']['software_config']['optional_components']
    assert Component['ANACONDA'].value not in config_built['config']['software_config']['optional_components']

    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file_2_0_with_anaconda)

    config_built = spawner._build_cluster_config()
    assert Component['JUPYTER'].value in config_built['config']['software_config']['optional_components']
    assert Component['ANACONDA'].value not in config_built['config']['software_config']['optional_components']

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
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "get_username", test_username)

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
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

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
    assert config_built['config']['software_config']['properties']['dataproc:jupyter.hub.env'] == 'test-env-str'
    assert config_built['config']['software_config']['properties']['dataproc:jupyter.hub.menu.enabled'] == 'true'
    assert 'dataproc:jupyter.hub.enabled' not in config_built['config']['software_config']['properties']
    assert 'dataproc:dataproc.personal-auth.user' not in config_built['config']['software_config']['properties']

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
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.show_spawned_clusters_in_notebooks_list = False
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
    assert config_built['config']['endpoint_config']['enable_http_port_access'] == True
    # Verify that we disable preemptibility (temporarily)
    assert 'preemptibility' not in config_built['config']['master_config']
    assert 'preemptibility' not in config_built['config']['worker_config']
    # Verify that we removed cluster-specific namenode properties
    assert 'hdfs:dfs.namenode.lifeline.rpc-address' not in config_built['config']['software_config']['properties']
    assert 'hdfs:dfs.namenode.servicerpc-address' not in config_built['config']['software_config']['properties']
    # Verify that notebook tag is disabled
    assert config_built['config']['software_config']['properties']['dataproc:jupyter.instance-tag.enabled'] is 'false'

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
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)

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
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

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
            'key_with_underscore_too': 'some_UPPER_and_UlowerU:1234',
            'session-user': spawner.get_username()
          },
          'zone_uri': 'https://www.googleapis.com/compute/v1/projects/test-project/zones/test-form1-a'
        },
        'initialization_actions': [],
        'lifecycle_config': {},
        'master_config': {
          'machine_type_uri': 'machine.1.2_numbers',
          'min_cpu_platform': 'AUTOMATIC',
          'disk_config': {
            'boot_disk_size_gb': 1000
          },
        },
        'worker_config': {},
        'software_config': {
          'image_version': '1.4-debian9',
          'optional_components': [
              Component.JUPYTER.value,
              Component.ANACONDA.value],
          'properties': {
            'dataproc:jupyter.hub.args': 'test-args-str',
            'dataproc:jupyter.hub.env': 'test-env-str',
            'dataproc:jupyter.hub.menu.enabled': 'true',
            'dataproc:jupyter.instance-tag.enabled': 'false',
            'dataproc:jupyter.notebook.gcs.dir': f'gs://users-notebooks/{spawner.get_username()}',
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
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

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
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

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
      'm2': 'v2',
      'session-user': spawner.get_username()
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

    def test_read_file_network(*args, **kwargs):
      config_string = open('./tests/test_data/basic_with_network.yaml', 'r').read()
      return config_string

    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file_string)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

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
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file_network)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a'
    }

    config_built = spawner._build_cluster_config()
    assert 'subnetwork_uri' not in config_built['config']['gce_cluster_config']

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
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

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

  def test_image_version_supports_anaconda(self):
    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    assert spawner._image_version_supports_anaconda('1.3') is True
    assert spawner._image_version_supports_anaconda('1.3-debian9') is True
    assert spawner._image_version_supports_anaconda('1.3.6-debian9') is True
    assert spawner._image_version_supports_anaconda('1.3.59-debian9') is True
    assert spawner._image_version_supports_anaconda('1.3.999-debian9') is True
    assert spawner._image_version_supports_anaconda('1.4-debian10') is True
    assert spawner._image_version_supports_anaconda('1.4.6-debian10') is True
    assert spawner._image_version_supports_anaconda('1.4.31-debian10') is True
    assert spawner._image_version_supports_anaconda('1.5-debian10') is True
    assert spawner._image_version_supports_anaconda('1.5.0-debian10') is True
    assert spawner._image_version_supports_anaconda('1.5.5-debian10') is True
    assert spawner._image_version_supports_anaconda('2') is False
    assert spawner._image_version_supports_anaconda('2.0') is False
    assert spawner._image_version_supports_anaconda('2.0-ubuntu18') is False
    assert spawner._image_version_supports_anaconda('2.0.0') is False
    assert spawner._image_version_supports_anaconda('2.0.0-debian10') is False
    assert spawner._image_version_supports_anaconda('2.0.1') is False
    assert spawner._image_version_supports_anaconda('2.3.0') is False
    assert spawner._image_version_supports_anaconda('2.0.0-RC1-debian10') is True
    assert spawner._image_version_supports_anaconda('2.0.0-RC7-debian10') is True
    assert spawner._image_version_supports_anaconda('2.0.0-RC11-debian10') is True
    assert spawner._image_version_supports_anaconda('2.0.0-RC12-debian10') is False
    assert spawner._image_version_supports_anaconda('2.0.0-RC77-debian10') is False
    assert spawner._image_version_supports_anaconda('weird-unexpected-version-124.3.v2.2020-02-15') is True
    assert spawner._image_version_supports_anaconda('1.3.weird-version-again') is True

  def test_validate_proto(self, monkeypatch):
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/unknown_fields.yaml', 'r').read()
      return config_string

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)

    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'basic_uri.yaml',
      'cluster_zone': 'us-east1-d'
    }

    cleaned_config = spawner.get_cluster_definition('')
    warnings = dataprocspawner.spawner._validate_proto(cleaned_config, Cluster)

    # Check that we had appropriate warning messages
    assert len(warnings) == 7
    expected_warnings = [
       'Removing unknown/bad value BAD_ENUM_VALUE for field consume_reservation_type.',
       "Removing unknown field unknown_field for class <class 'google.cloud.dataproc_v1beta2.types.clusters.NodeInitializationAction'>",
       'Removing unknown/bad value UNKNOWN_COMPONENT_1 for field optional_components.',
       'Removing unknown/bad value UNKNOWN_COMPONENT_2 for field optional_components.',
       'Removing unknown/bad value UNKNOWN_COMPONENT_3 for field optional_components.',
       "Removing unknown field unknown_field_config_level for class <class 'google.cloud.dataproc_v1beta2.types.clusters.ClusterConfig'>",
       "Removing unknown field unknown_field_top_level for class <class 'google.cloud.dataproc_v1beta2.types.clusters.Cluster'>",
    ]
    for w in expected_warnings:
      assert w in warnings, f'Expected message {w} in warnings {warnings}'

    raw_config = spawner.get_cluster_definition('')
    # Construct expected output
    del raw_config['unknown_field_top_level']
    del raw_config['config']['unknown_field_config_level']
    del raw_config['config']['initialization_actions'][0]['unknown_field']
    del raw_config['config']['gce_cluster_config']['reservation_affinity']['consume_reservation_type']
    raw_config['config']['software_config']['optional_components'] = [
        'JUPYTER', 'ZEPPELIN', 'ANACONDA', 'PRESTO']

    # Coerce both of the outputs to proto so we can easily compare equality
    # this also sanity checks that we have actually stripped all unknown/bad
    # fields
    actual_proto = Cluster(cleaned_config)
    expected_proto = Cluster(raw_config)

    assert actual_proto == expected_proto

    # Now check that the config with resolved fields is correct as well
    config_built = spawner._build_cluster_config()

    assert 'unknown_field_top_level' not in config_built
    assert 'unknown_field_config_level' not in config_built['config']
    assert 'unknown_field' not in config_built['config']['initialization_actions'][0]
    assert 'consume_reservation_type' not in config_built['config']['gce_cluster_config']['reservation_affinity']
    assert  raw_config['config']['software_config']['optional_components'] == [
        'JUPYTER', 'ZEPPELIN', 'ANACONDA', 'PRESTO']

  @pytest.mark.asyncio
  async def test_progress(self, monkeypatch):
    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_logging_client = mock.create_autospec(
        logging_v2.LoggingServiceV2Client(credentials=fake_creds))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(),
                              _mock=True, logging=mock_logging_client,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-progress')

    async def test_get_cluster_notebook_endpoint(*args, **kwargs):
      await asyncio.sleep(0)
      return f'https://abcd1234-dot-{self.region}.dataproc.googleusercontent.com/jupyter'

    monkeypatch.setattr(spawner, "get_cluster_notebook_endpoint", test_get_cluster_notebook_endpoint)

    async def collect(ait):
      items = []
      async for value in ait:
        items.append(value)
      return items

    def create_logs():
      entries = []
      for i in range(5):
        e = LogEntry(
          insert_id=f'entry_{i}',
          json_payload=ParseDict({'method':'method', 'message':f'message_{i}'}, Struct())
        )
        entries.append(e)
      return entries

    def create_expected():
      progress = 0
      expected = []
      i = 0
      for e in create_logs():
        progress += math.ceil((90 - progress) / 4)
        expected.append({'progress': progress,'message': f'method: message_{i}'})
        i += 1
      return expected

    def test_list_log_entries(*args, **kwargs):
      return create_logs()

    op = MockOperation('op1', 'cluster1-op1')

    monkeypatch.setattr(mock_logging_client, 'list_log_entries', test_list_log_entries)
    monkeypatch.setattr(spawner, 'operation', op)

    await spawner.start()
    assert await collect(spawner.progress()) == create_expected()

  @pytest.mark.asyncio
  async def test_old_progress_recovery(self, monkeypatch):
    fake_creds = AnonymousCredentials()
    mock_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_logging_client = mock.create_autospec(
        logging_v2.LoggingServiceV2Client(credentials=fake_creds))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(),
                              _mock=True, logging=mock_logging_client,
                              gcs_notebooks=self.gcs_notebooks, compute=mock_compute_client, project='test-progress')

    def test_clustername(*args, **kwargs):
      return 'test-clustername'

    async def test_get_cluster_notebook_endpoint(*args, **kwargs):
      await asyncio.sleep(0)
      return f'https://abcd1234-dot-{self.region}.dataproc.googleusercontent.com/jupyter'

    monkeypatch.setattr(spawner, "get_cluster_notebook_endpoint", test_get_cluster_notebook_endpoint)
    monkeypatch.setattr(spawner, "clustername", test_clustername)

    async def collect(ait):
      items = []
      async for value in ait:
        items.append(value)
      return items

    yields_start = [
      {'message': 'Server requested.', 'progress': 0},
      {'message': 'Operation op1 for cluster uuid cluster1-op1', 'progress': 5}
    ]

    yields_existing = [
      {'progress': 0, 'message': 'Message 1.'},
      {'progress': 40, 'message': 'Message 2.'},
      {'progress': 70, 'message': 'Message 3.'},
    ]

    progressor = {
      'test-clustername': SimpleNamespace(bar=0, logging=set(), start='', yields=yields_existing)
    }

    op = MockOperation('op1', 'cluster1-op1')

    monkeypatch.setattr(spawner, '_spawn_pending', lambda: True)
    monkeypatch.setattr(spawner, 'progressor', progressor)
    monkeypatch.setattr(spawner, 'operation', op)

    await spawner.start()
    assert await collect(spawner._generate_progress()) == yields_existing

  def test_user_options_image_version(self, monkeypatch):
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/minimum.yaml', 'r').read()
      return config_string

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)

    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.allow_custom_clusters = True
    spawner.user_options = {
      'cluster_type': 'minimum.yaml',
      'cluster_zone': 'test-form1-a',
      'custom_cluster': '1',
      'image_version': '1.5-debian10'
    }

    config_built = spawner._build_cluster_config()

    assert config_built['config']['software_config']['image_version'] == '1.5-debian10'

  def test_user_options_custom_image(self, monkeypatch):
    import yaml

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/basic.yaml', 'r').read()
      return config_string

    def test_image_version(*args, **kwargs):
      image_version = '1.5-debian10'
      return image_version

    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    # Mock the Compute Engine API client
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')
    # Prevents a call to GCS. We return the local file instead.
    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)

    spawner.region = "us-east1"
    spawner.zone = "us-east1-d"
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.allow_custom_clusters = True
    spawner.user_options = {
      'cluster_type': 'basic.yaml',
      'cluster_zone': 'test-form1-a',
      'custom_cluster': '1',
      'image_version': 'custom',
      'custom_image': 'projects/test-project/global/images/custom-image'
    }

    monkeypatch.setattr(spawner, '_get_image_version', test_image_version)

    config_built = spawner._build_cluster_config()

    assert config_built['config']['software_config']['image_version'] == '1.5-debian10'
    assert config_built['config']['master_config']['image_uri'] == 'projects/test-project/global/images/custom-image'

  ##
  # Tests for personal auth
  ##
  def test_personal_auth_flag(self, monkeypatch):
    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')

    spawner.force_single_user = True
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    config_built = spawner._build_cluster_config()
    assert (config_built['config']['software_config']['properties']
        ['dataproc:dataproc.personal-auth.user']) == spawner.user.name

  def test_personal_auth_yaml(self, monkeypatch):
    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/perso.yaml', 'r').read()
      return config_string

    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.user_options = {
      'cluster_type': 'perso.yaml',
      'cluster_zone': 'us-central-1'
    }

    config_built = spawner._build_cluster_config()

    assert (config_built['config']['software_config']['properties']
        ['dataproc:dataproc.personal-auth.user']) == spawner.user.name

  def test_personal_auth_user(self, monkeypatch):
    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/perso.yaml', 'r').read()
      return config_string

    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    spawner.allow_custom_clusters = True
    spawner.user_options = {
      'cluster_type': 'perso.yaml',
      'cluster_zone': 'us-central-1',
      "cluster_props_prefix_0": "dataproc",
      "cluster_props_key_0": "dataproc.personal-auth.user",
      "cluster_props_val_0": "user@example.com"
    }
    config_built = spawner._build_cluster_config()
    assert (config_built['config']['software_config']['properties']
        ['dataproc:dataproc.personal-auth.user']) == spawner.user.name

  def test_personal_auth_chechbox_on(self, monkeypatch):
    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')

    def test_read_file(*args, **kwargs):
      config_string = open('./tests/test_data/perso.yaml', 'r').read()
      return config_string

    monkeypatch.setattr(spawner, "read_gcs_file", test_read_file)
    spawner.user_options = {
      'cluster_type': 'minimum.yaml',
      'cluster_zone': 'us-central-1',
      'personal_auth': 'on'
    }
    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    config_built = spawner._build_cluster_config()
    assert (config_built['config']['software_config']['properties']
        ['dataproc:dataproc.personal-auth.user']) == spawner.user.name

  def test_personal_auth_chechbox_off(self, monkeypatch):
    fake_creds = AnonymousCredentials()
    mock_dataproc_client = mock.create_autospec(ClusterControllerClient(credentials=fake_creds))
    mock_gcs_client = mock.create_autospec(storage.Client(credentials=fake_creds, project='project'))
    mock_compute_client = mock.create_autospec(discovery.build('compute', 'v1',
                                               credentials=fake_creds, cache_discovery=False))
    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_dataproc_client, gcs=mock_gcs_client,
                              user=MockUser(), _mock=True, gcs_notebooks=self.gcs_notebooks,
                              compute=mock_compute_client, project='test-project')

    spawner.env_str = "test-env-str"
    spawner.args_str = "test-args-str"
    config_built = spawner._build_cluster_config()
    assert 'dataproc:dataproc.personal-auth.user' not in config_built['config']['software_config']['properties']


