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

FROM marketplace.gcr.io/google/debian10:latest

# Installs gcloud.
RUN apt-get update && apt-get install -y curl apt-transport-https ca-certificates gnupg \
  && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
  && echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
  && apt-get update && apt-get install -y google-cloud-sdk

# Insalls libraries for Dataproc Hub
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    libssl-dev \
    libpq-dev \
    libcurl4-openssl-dev \
    python3-dev \
    python3-pip \
    python3-pycurl \
    python3-setuptools \
    python3-wheel \
    git \
    vim \
    iproute2 \
    node-gyp \
    libnode-dev \
    npm \
    nodejs \
  && apt-get purge \
  && apt-get clean -y

RUN pip3 install --upgrade setuptools pip wheel

RUN pip install --upgrade \
    psycopg2-binary \
    google-api-python-client \
    google-auth-oauthlib \
    google-api-python-client \
    oauth2client \
    google-auth \
    googleapis-common-protos \
    google-auth-httplib2 \
    jupyterhub==1.2.2

RUN npm install -g configurable-http-proxy

# Install Dataproc Spawner
COPY setup.py /tmp/dataprocspawner/setup.py
COPY dataprocspawner/ /tmp/dataprocspawner/dataprocspawner/
COPY dataprochub/ /tmp/dataprocspawner/dataprochub/
RUN pip install git+https://github.com/GoogleCloudPlatform/jupyterhub-gcp-proxies-authenticator
RUN pip install /tmp/dataprocspawner/

# Dataproc Hub design
COPY static/templates/ /etc/jupyterhub/templates/
COPY static/mdc/ /usr/local/share/jupyterhub/static/mdc/

COPY docker/jupyterhub_config.py /etc/jupyterhub/jupyterhub_config.py

EXPOSE 8080

LABEL "com.google.environment"="Dataproc Hub"

COPY docker/jupyterhub.sh /usr/local/bin/
ENTRYPOINT ["bash", "-C", "/usr/local/bin/jupyterhub.sh"]
