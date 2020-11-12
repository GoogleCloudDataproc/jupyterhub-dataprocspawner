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

"""Sets up package for DataprocSpawner

This package should only be used within a jupyterhub_config.py file.
"""
import os

from setuptools import setup

pjoin = os.path.join

packages = ['dataprocspawner']
for d, _, _ in os.walk('dataprochub'):
  if os.path.exists(pjoin(d, '__init__.py')):
    packages.append(d.replace(os.path.sep, '.'))

setup(
  name='jupyterhub-dataprocspawner',
  packages=packages,
  python_requires='>=3.6.0',
  version='0.1',
  description='DataprocSpawner for JupyterHub',
  url='https://github.com/GoogleCloudPlatform/dataprocspawner',
  license='Apache 2.0',
  install_requires=[
    'tornado>=5.0',
    'google-cloud-dataproc>=2.0.0',
    'google-cloud-storage>=1.25.0',
    'traitlets>=4.3.2',
    'google-cloud-core>=1.3.0',
    'google-cloud-secret-manager>=0.1.1',
    'google-cloud-logging>=1.15'
    'pyyaml>=5.1.2',
    'oauthenticator>=0.9.0',
    'pyjwt>=1.7.1'
  ],
  entry_points={
    'console_scripts': [
      'jupyterhub = dataprochub.app:main',
      'jupyterhub-singleuser = jupyterhub.singleuser:main',
    ],
    'jupyterhub.proxies': [
      'redirect-proxy = dataprochub.proxy:RedirectProxy',
    ],
  },
  setup_requires=['pytest-runner'],
  tests_require=['pytest', 'pytest-asyncio'],
)
