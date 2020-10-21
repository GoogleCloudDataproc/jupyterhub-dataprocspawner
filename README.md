# DataprocSpawner

DataprocSpawner enables [JupyterHub][jupyterhub] to spawn single-user [jupyter_notebooks][Jupyter notebooks] that run on [Dataproc][dataproc] clusters. This provides users with ephemeral clusters for data science without the pain of managing them.

- [Product Documentation][dataproc]
- DISCLAIMER: DataprocSpawner only supports zonal DNS names. If your project uses global DNS names, click [this][dns] for instructions on how to migrate.

[jupyterhub]: https://jupyterhub.readthedocs.io/en/stable/
[jupyter_notebooks]: https://jupyter-notebook-beginner-guide.readthedocs.io/en/latest/what_is_jupyter.html
[dataproc]: https://cloud.google.com/dataproc
[dns]: https://cloud.google.com/compute/docs/internal-dns#migrating-to-zonal

Supported Python Versions: Python >= 3.6

## Before you begin

In order to use this library, you first need to go through the following steps:

1. [Select or create a Cloud Platform project][create_project]
2. [Enable billing for your project][enable_billing]
3. [Enable the Google Cloud Dataproc API][enable_api]
4. [Setup Authentication][authentication]

[create_project]: https://console.cloud.google.com/project
[enable_billing]: https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project
[enable_api]: https://cloud.google.com/dataproc
[authentication]: https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-python

## Installation example

### Locally

To try is locally for development purposes. From the root folder:

```sh
chmod +x deploy_local_example.sh
./deploy_local_example.sh <YOU_PROJECT_ID> <YOUR_GCS_CONFIG_LOCATIONS> <YOUR_AUTHENTICATED_EMAIL>
```

The script will start a local container image and authenticate it using your local credentials.

Note: Although you can try the Dataproc Spawner image locally, you might run into networking communication problems.

### Google Compute Engine

To try it out in the Cloud, the quickest way is to to use a test Compute Engine instance. The following takes you through the process.

1. Set your working project

    ```bash
    PROJECT_ID=<YOUR_PROJECT_ID>
    VM_NAME=vm-spawner
    ```

1. Run the example script which:

    a. Creates a Dockerfile
    b. Creates a jupyter_config.py example file that uses a dummy authenticator.
    c. Deploy a Docker image of the JupyterHub spawner in [Google Container Registry][gcr]
    d. Create a container-based Compute Engine
    e. Returns the IP of the instance that runs JupyterHub.

    ```bash
    bash deploy_gce_example.sh ${PROJECT_ID} ${VM_NAME}
    ```

1. After the script finishes, you should see an IP displayed. You can use that IP to access your setup at `<IP>:8000`. You might have to wait for a few minutes until the container is deployed on the instance.

[gcr]: https://cloud.google.com/container-registry/

## Troubleshooting

To troubleshoot

1. ssh into the VM:

    ```bash
    gcloud compute ssh ${VM_NAME}
    ```

1. From the VM console, install some useful tools:

    ```bash
    apt-get update
    apt-get install vim
    ```

1. From the VM console, you can:

    - List the running containers with `docker ps`
    - Display container logs `docker logs -f <CONTAINER_ID>`
    - Execute code in the container `docker exec -it <CONTAINER_ID> /bin/bash`
    - Restart the container for changes to take effect  `docker restart <CONTAINER_ID>`

## Notes

- DataprocSpawner defaults to port 12345, the port can be set within `jupyterhub_config.py`. More info in JupyterHub's [jupyterhub_documentation].

    c.Spawner.port = {port number}

- The region default is ``us-central1`` for Dataproc clusters. The zone default is ``us-central1-a``. Using ``global`` is currently unsupported. To change region, pick a region and zone from this [list][locations_list] and include the following lines in ``jupyterhub_config.py``:

    .. code-block:: console

    c.DataprocSpawner.region = '{region}'
    c.DataprocSpawner.zone = '{zone that is within the chosen region}'

[jupyterhub_documentation]: https://jupyterhub.readthedocs.io/en/stable/api/spawner.html#jupyterhub.spawner.Spawner.port
[locations_list]: https://cloud.google.com/compute/docs/regions-zones/

## Next

- For an example on how to run the Dataproc spawner in production, refer to the [ai-notebook-extended Github repository](https://github.com/GoogleCloudPlatform/ai-notebooks-extended).
- For a Google-supported version of the Dataproc Spawner, refer to the official [Dataproc Hub documentation](https://cloud.google.com/dataproc/docs/tutorials/dataproc-hub-admins).

## Disclaimer

[This is not an official Google product.](https://opensource.google.com/docs/releasing/publishing/#disclaimer)
