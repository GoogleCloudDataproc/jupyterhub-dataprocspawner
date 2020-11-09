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
from dataprochub.app import DataprocHub, DataprocHubUserUrlHandler
from dataprochub.proxy import RedirectProxy
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
from jupyterhub.objects import Hub, Server
from unittest import mock
from types import SimpleNamespace
from traitlets import Any
from jupyterhub import handlers, apihandlers


class TestDataprocHub:

  def test_handler_outputs(self, monkeypatch):

    def make_tuple(a):
      if len(a) == 3:
        return (a[0], eval(a[1]), eval(a[2]))
      return (a[0], eval(a[1]))

    mock_app = DataprocHub()
    mock_app.db = Any()

    handlers_given = open('./tests/test_data/handlers_given.json', 'r').read()
    handlers_expected = open('./tests/test_data/handlers_expected.json', 'r').read()

    handlers_given = json.loads(handlers_given)
    handlers_expected = json.loads(handlers_expected)

    handlers_given = [make_tuple(h) for h in handlers_given]
    handlers_expected = [make_tuple(h) for h in handlers_expected]

    monkeypatch.setattr(mock_app, 'handlers', handlers_given)
    monkeypatch.setattr(mock_app, 'logo_file', 'local')

    mock_app.init_handlers()
    handlers_modified = mock_app.handlers

    assert handlers_modified == handlers_expected

  def test_handler_order(self, monkeypatch):

    def make_tuple(a):
      if len(a) == 3:
        return (a[0], eval(a[1]), eval(a[2]))
      return (a[0], eval(a[1]))

    mock_app = DataprocHub()
    mock_app.db = Any()

    handlers_given = open('./tests/test_data/handlers_given.json', 'r').read()
    handlers_expected = open('./tests/test_data/handlers_expected.json', 'r').read()

    handlers_given = json.loads(handlers_given)
    handlers_expected = json.loads(handlers_expected)

    handlers_given = [make_tuple(h) for h in handlers_given]
    handlers_expected = [make_tuple(h) for h in handlers_expected]

    monkeypatch.setattr(mock_app, 'handlers', handlers_given)
    monkeypatch.setattr(mock_app, 'logo_file', 'local')

    mock_app.init_handlers()
    handlers_modified = mock_app.handlers

    idx_hub_user = 0
    idx_hub_redirect = 0
    for idx, h in enumerate(handlers_modified):
      rgx = h[0]
      if rgx == r'/hub/user/(?P<user_name>[^/]+)(?P<user_path>/.*)?':
        idx_hub_user = idx

      if rgx == r'(?!/hub/).*':
        idx_hub_redirect = idx

    assert idx_hub_user != idx_hub_redirect
    assert idx_hub_redirect != 0
    assert idx_hub_user < idx_hub_redirect


  @pytest.mark.asyncio
  async def test_proxy_route(self, monkeypatch):

    mock_proxy = RedirectProxy()

    template = {
      '/': {
        'jupyterhub': True,
        'routespec': '/',
        'target': 'http://172.17.0.2:8081',
        'data': {
          'hub': True,
          'last_activity': '2020-01-01T01:00:00.000Z',
        }
      },
    }

    new_route = {
      '/new':  {
        'jupyterhub': True,
        'routespec': '/new',
        'target': 'http://172.17.0.2:8082',
        'data': {
          'hub': True,
          'last_activity': '2020-02-02T02:00:00.000Z',
        }
      }
    }

    @pytest.mark.asyncio
    async def test_api_request(*args, **kwargs):

      # get_all_routes()
      if args[0] == '':
        await asyncio.sleep(0)
        resp_body = json.dumps(routes).encode('utf-8')
        resp = SimpleNamespace(body=resp_body)
        return resp

      # add_routes()
      await asyncio.sleep(0)
      routes = {**template, **new_route}

    monkeypatch.setattr(mock_proxy, 'api_request', test_api_request)

    rs_ok = '/route_allowed'
    rs_ko = 'https://fml2ffthgjd63j4nn7x6tyttju-dot-us-central1.dataproc.googleusercontent.com/gateway/default/jupyter/tree?'

    # Test non Component Gateway URL are added in the routes.
    ok = await mock_proxy.add_route(routespec='/user/mock/', target=rs_ok, data={})
    assert ok == None

    # Tests that Component Gateway is not added in the routes.
    ko = await mock_proxy.add_route(routespec='/user/mock/', target=rs_ko, data={})
    assert ko == rs_ko
