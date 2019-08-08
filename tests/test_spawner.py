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

""" Unit tests for DataprocSpawner.

Unit tests for methods within DataprocSpawner (start, stop, and poll).
"""
from collections import namedtuple
from dataprocspawner import DataprocSpawner
from google.cloud import dataproc_v1beta2
from google.cloud.dataproc_v1beta2.proto import clusters_pb2
from google.longrunning import operations_pb2
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
    assert ip == f'fake-jupyterhub-m.{self.zone}.c.{spawner.project}.internal'
    # JupyterHub defaults to 0 if no port set
    assert port == 0

    mock_client.create_cluster.assert_called_once()
    assert spawner.cluster_data['cluster_name'] == 'fake-jupyterhub'
    assert spawner.cluster_data['config']['gce_cluster_config']['zone_uri'] == f'https://www.googleapis.com/compute/v1/projects/{spawner.project}/zones/{spawner.zone}'

    env = json.loads(spawner.cluster_data['config']['software_config']['properties']['dataproc:jupyter.hub.env'])
    assert env['JUPYTERHUB_API_URL'] is not None

  @pytest.mark.asyncio
  async def test_start_existing_clustername(self):

    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "test-create-existing"
    assert spawner.project == "test-create-existing"

    (ip, port) = await spawner.start()
    assert ip == f'fake-jupyterhub-m.{self.zone}.c.{spawner.project}.internal'
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

    mock_client.delete_cluster.assert_called_once_with("test-stop", self.region, 'fake-jupyterhub')

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
    assert ip == f'fake-jupyterhub-m.{self.zone}.c.{spawner.project}.internal'
    assert port == 0

  @pytest.mark.asyncio
  async def test_domain_scoped_zonal_dns(self):
    mock_client = mock.create_autospec(dataproc_v1beta2.ClusterControllerClient())

    spawner = DataprocSpawner(hub=Hub(), dataproc=mock_client, user=MockUser(), _mock=True)

    spawner.project = "test:domain-scoped"
    assert spawner.project == "test:domain-scoped"

    (ip, port) = await spawner.start()
    assert ip == f'fake-jupyterhub-m.{self.zone}.c.domain-scoped.test.internal'
    assert port == 0




