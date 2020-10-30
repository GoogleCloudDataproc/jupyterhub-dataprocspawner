FROM jupyterhub/jupyterhub

# Install gcloud
RUN apt-get update && apt-get install -y curl apt-transport-https ca-certificates gnupg   && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -   && echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list   && apt-get update && apt-get install -y google-cloud-sdk

RUN apt-get update   && apt-get remove -y python3-pycurl   && apt-get install -y --no-install-recommends     libssl-dev     libcurl4-openssl-dev     python3-wheel     git     tcpdump   && apt-get purge   && apt-get clean -y

RUN pip install --upgrade pip   && pip install --upgrade     psycopg2-binary     google-api-python-client     google-auth-oauthlib     google-api-python-client     oauth2client     google-auth     googleapis-common-protos     google-auth-httplib2

RUN apt-get update && apt-get install -y libpq-dev     && apt-get install -y vim     && apt-get install -y iproute2     && apt-get autoremove -y     && apt-get clean -y

RUN pip install jupyterhub-dummyauthenticator

COPY jupyterhub_config.py .
COPY . dataprocspawner/
COPY dataprochub dataprochub

RUN cd dataprocspawner && pip install .

RUN pip install git+https://github.com/GoogleCloudPlatform/jupyterhub-gcp-proxies-authenticator.git

EXPOSE 8080

ENTRYPOINT ["jupyterhub"]
