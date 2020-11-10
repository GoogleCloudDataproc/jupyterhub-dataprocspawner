FROM gcr.io/deeplearning-platform-release/base-cpu

# Installs JupyterHub.
RUN apt-get update \
  && apt-get install -y \
    vim \
    node-gyp \
    nodejs-dev \
    libssl1.0-dev \
    npm \
    nodejs

RUN pip install --upgrade pip \
  && pip install --upgrade \
    jupyterhub

RUN npm install -g configurable-http-proxy

# Installs gcloud.
RUN apt-get update && apt-get install -y curl apt-transport-https ca-certificates gnupg \
  && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
  && echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
  && apt-get update && apt-get install -y google-cloud-sdk

RUN apt-get update \
  && apt-get remove -y python3-pycurl \
  && apt-get install -y --no-install-recommends \
    libssl-dev \
    libcurl4-openssl-dev \
    python3-wheel \
    git \
  && apt-get purge \
  && apt-get clean -y

RUN pip install --upgrade pip \
  && pip install --upgrade \
    psycopg2-binary \
    google-api-python-client \
    google-auth-oauthlib \
    google-api-python-client \
    oauth2client \
    google-auth \
    googleapis-common-protos \
    google-auth-httplib2

RUN apt-get update && apt-get install -y libpq-dev \
    && apt-get install -y vim \
    && apt-get install -y iproute2 \
    && apt-get autoremove -y \
    && apt-get clean -y

# Install Dataproc Spawner
COPY setup.py /tmp/dataprocspawner/setup.py
COPY dataprocspawner/ /tmp/dataprocspawner/dataprocspawner/
COPY dataprochub/ /tmp/dataprocspawner/dataprochub/
RUN pip install git+https://github.com/GoogleCloudPlatform/jupyterhub-gcp-proxies-authenticator
RUN pip install /tmp/dataprocspawner/

# JupyterHub design
COPY static/templates/ /etc/jupyterhub/templates/

COPY docker/jupyterhub_config.py .

EXPOSE 8080

LABEL "com.google.environment"="Dataproc Hub"

COPY docker/jupyterhub.sh /usr/local/bin/
ENTRYPOINT ["bash", "-C", "/usr/local/bin/jupyterhub.sh"]
