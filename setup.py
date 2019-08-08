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

"""Sets up package for DataprocSpawner

This package should only be used within a jupyterhub_config.py file.
"""

from setuptools import setup

setup(
    name='jupyterhub-dataprocspawner',
    python_requires='>=3.6.0',
    version='0.1',
    description='DataprocSpawner for JupyterHub',
    url='https://github.com/GoogleCloudPlatform/dataprocspawner',
    author='Ann Yue',
    author_email='annyue@google.com',
    license='Apache 2.0',
    packages=['dataprocspawner'],
    install_requires=[
        'jupyterhub>=1.0.0',
        'tornado>=5.0',
        'google-cloud-dataproc>=0.4.0',
        'traitlets>=4.3.2',
        'google-cloud-core>=1.0.2'
    ],
    setup_requires=["pytest-runner"],
    tests_require=["pytest", "pytest-asyncio"],
)