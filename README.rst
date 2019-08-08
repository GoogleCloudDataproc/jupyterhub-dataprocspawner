DataprocSpawner
===============

DataprocSpawner enables `JupyterHub`_ to spawn single-user `Jupyter notebooks`_ that run on `Dataproc`_ clusters. This provides users with ephemeral clusters for data science without the pain of managing them.

- `Product Documentation`_
- DISCLAIMER: DataprocSpawner only supports zonal DNS names. If your project uses global DNS names, click `this`_ for instructions on how to migrate.

.. _JupyterHub: https://jupyterhub.readthedocs.io/en/stable/
.. _Jupyter notebooks: https://jupyter-notebook-beginner-guide.readthedocs.io/en/latest/what_is_jupyter.html
.. _Dataproc: https://cloud.google.com/dataproc
.. _Product Documentation: https://cloud.google.com/dataproc
.. _this: https://cloud.google.com/compute/docs/internal-dns#migrating-to-zonal

Quick Start
-----------

In order to use this library, you first need to go through the following steps:

1. `Select or create a Cloud Platform project.`_
2. `Enable billing for your project.`_
3. `Enable the Google Cloud Dataproc API.`_
4. `Setup Authentication.`_

.. _Select or create a Cloud Platform project.: https://console.cloud.google.com/project
.. _Enable billing for your project.: https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project
.. _Enable the Google Cloud Dataproc API.:  https://cloud.google.com/dataproc
.. _Setup Authentication.: https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-python

Installation
~~~~~~~~~~~~

Supported Python Versions
^^^^^^^^^^^^^^^^^^^^^^^^^
Python >= 3.6

Linux
^^^^^

.. code-block:: console

    git clone https://github.com/GoogleCloudPlatform/dataprocspawner
    cd dataprocspawner && pip install .


Configuration
~~~~~~~~~~~~~
1. Generate ``jupyterhub_config.py`` for JupyterHub

    .. code-block:: console

            jupyterhub --generate-config
2. Within ``jupyterhub_config.py``, set the spawner and `GCP project`_. The project **must** be set.

    .. code-block:: console

            c.JupyterHub.spawner_class = 'dataprocSpawner.DataprocSpawner'
            c.DataprocSpawner.project = '{GCP project ID}'

.. _`GCP project`: https://cloud.google.com/resource-manager/docs/creating-managing-projects

Start JupyterHub
~~~~~~~~~~~~~~~~
To start JupyterHub, run the command:

    .. code-block:: console

        jupyterhub

Visit ``https://localhost:8000`` in your browser, and sign in with your unix credentials.

* Running JupyterHub locally is likely to run into issues due to firewall rules, use at your own risk/configuration.

Using Google Compute Engine (GCE)
---------------------------------------
To lessen the headache of setup, use the spawner by running JupyterHub within a Docker container that lives in a GCE VM.

`Google Container Registry`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Consider storing images on GCR for ease of deployment through GCE.

.. _`Google Container Registry`: https://cloud.google.com/container-registry/

Starting a GCE VM
~~~~~~~~~~~~~~~~~
The VM must allow full access to all Cloud APIs for the spawner to work.

Google Cloud SDK
^^^^^^^^^^^^^^^^
1. Install gcloud, part of the `Google Cloud SDK`_
2. `Create`_ a VM on GCE

  * If using a Docker image hosted on GCR:

    .. code-block:: console

        gcloud beta compute instances create-with-container {VM name} --container-image={image URL} --container-arg="--DataprocSpawner.project={GCP project ID}" --scopes=cloud-platform --zone us-central1-a

    - The spawned notebook by default listens to port 12345 for a connection from the hub. To set a custom port, include this in the gcloud command:

      .. code-block:: console

          --container-args="--Spawner.port={port number}"


  * If manually building the Docker image:

    .. code-block:: console

        gcloud beta compute instances create {VM name} --image-family=cos-stable --image-project=cos-cloud --scopes=cloud-platform --zone us-central1-a

    - The `container-optimized OS`_ comes with Docker preinstalled.
    - The full list of zones can be found `here`_


.. _`Google Cloud SDK`: https://cloud.google.com/sdk/docs/
.. _`Create`: https://cloud.google.com/sdk/gcloud/reference/compute/instances/create-with-container
.. _`container-optimized OS`: https://cloud.google.com/container-optimized-os/
.. _`here`: https://cloud.google.com/compute/docs/regions-zones/#available

Google Cloud Platform Console
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1. Visit the `Google Cloud Platform Console`_ for GCE
2. Create an instance

    .. image:: images/create.png
        :width: 500

  * Check the box under 'Container'

    .. image:: images/checkbox.png
        :width: 500

  * Provide the URL to the container image and set the project

    .. image:: images/config.png
        :width: 500

    - A custom port can also be set by adding another command argument

  * Set the access scopes

    .. image:: images/scope.png
        :width: 500

  * Hit create!

.. _`Google Cloud Platform Console`: https://console.cloud.google.com/compute

Configuration
~~~~~~~~~~~~~

1. SSH into the VM

  .. code-block:: console

            gcloud compute ssh {VM name}


Existing Docker Image
^^^^^^^^^^^^^^^^^^^^^
JupyterHub will be running once the VM has been created. No additional
commands are necessary. The following is for configuring JupyterHub.

* Find the name of the running Docker container

    .. code-block:: console

        docker ps

* Run bash in the running container

    .. code-block:: console

        docker exec -it {container name} bash

* Make changes as desired to ``jupyterhub_config.py`` (vim, cat, etc.) and exit the container

  - Installing vim while inside the conainer:

      .. code-block:: console

          apt-get update
          apt-get install vim


* Restart the container for changes in ``jupyterhub_config.py`` to take effect

    .. code-block:: console

        docker restart {container name}
* Check JupyterHub's logs to ensure changes took effect

    .. code-block:: console

        docker logs -f {container name}

Manual Docker Image
^^^^^^^^^^^^^^^^^^^
* Clone the DataprocSpawner repo, includes a ``Dockerfile`` and ``jupyterhub_config.py``
    .. code-block:: console

        git clone https://github.com/GoogleCloudPlatform/dataprocspawner
* Add additional configurations to either file, do **not** change the existing contents.

  - `Dockerfile`_
  - `jupyterhub_config.py`_

* If using a GCE instance running a container-optimized OS, allow connections from JupyterHub's REST API (`defaults to port 8081`_)
    .. code-block:: console

        sudo iptables -w -A INPUT -p tcp --dport 8081 -j ACCEPT

* Build a Docker image from the ``Dockerfile``
    .. code-block:: console

        docker build -t jupyterhub .
* Run a Docker container using the image
    .. code-block:: console

        docker run -it --net=host jupyterhub

  - The project can be passed as a container argument to Docker instead of setting it within ``jupyterhub_config.py``.
          .. code-block:: console

            docker run -it --net=host jupyterhub --DataprocSpawner.project={GCP project ID}

  - If the Docker image will be used repeatedly, consider `pushing the image to GCR`_.

Cloning the repo, building a Docker image, and pushing it to GCR can be done on a local machine.
Follow the instructions for an existing Docker image from above to then use the pushed image on GCE.

.. _`Dockerfile`: https://docs.docker.com/engine/reference/builder/
.. _`jupyterhub_config.py`: https://jupyterhub.readthedocs.io/en/stable/getting-started/config-basics.html
.. _`defaults to port 8081`: https://jupyterhub.readthedocs.io/en/stable/getting-started/networking-basics.html#set-the-proxy-s-rest-api-communication-url-optional
.. _`pushing the image to GCR`: https://cloud.google.com/container-registry/docs/pushing-and-pulling


Notes
-----

- DataprocSpawner defaults to port 12345, the port can be set within ``jupyterhub_config.py``. More info in JupyterHub's `documentation`_.

    .. code-block:: console

        c.Spawner.port = {port number}
- The region default is ``us-central1`` for Dataproc clusters. The zone default is ``us-central1-a``. Using ``global`` is currently unsupported.
  To change region, pick a region and zone from this `list`_ and include the following lines in ``jupyterhub_config.py``:

    .. code-block:: console

        c.DataprocSpawner.region = '{region}'
        c.DataprocSpawner.zone = '{zone that is within the chosen region}'

.. _`documentation`: https://jupyterhub.readthedocs.io/en/stable/api/spawner.html#jupyterhub.spawner.Spawner.port
.. _`list`: https://cloud.google.com/compute/docs/regions-zones/

Disclaimer
----------
`This is not an official Google product.`_

.. _`This is not an official Google product.`: https://opensource.google.com/docs/releasing/publishing/#disclaimer