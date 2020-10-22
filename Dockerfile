FROM jupyterhub/jupyterhub

RUN pip install jupyterhub-dummyauthenticator

COPY jupyterhub_config.py .

COPY . dataprocspawner/
RUN cd dataprocspawner && pip install .

COPY templates /etc/jupyterhub/templates

EXPOSE 8080

ENTRYPOINT ["jupyterhub"]
