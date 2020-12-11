#!/bin/bash
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

set +x

readonly metadata_base_url="http://metadata.google.internal/computeMetadata/v1"

# 'append-config-ain'
# Adds content to jupyterhub_config.py so JupyterHub
# can run on an AI Platform Notebook instance.
function append-config-ain {
  cat <<EOT >> jupyterhub_config.py
## Start of configuration for JupyterHub on AI Notebooks ##
c.Spawner.spawner_host_type = 'ain'

# Option on Dataproc Notebook server to allow authentication.
c.Spawner.args = ['--NotebookApp.disable_check_xsrf=True']
EOT
}

# 'append-config-testing'
# Provides a dummy email to authenticator to support non
# inverting proxy use cases. Generally for testing purposes.
function append-config-testing {
  cat <<EOT >> jupyterhub_config.py
## Start of configuration for JupyterHub on local machine ##
c.GCPProxiesAuthenticator.dummy_email = 'testing@example.com'
c.JupyterHub.log_level = 'DEBUG'
EOT
}

# 'append-to-jupyterhub-config'
# Checks if JupyterHub is running on an AI Platform Notebook instance
# and appends the proper configuration to jupyterhub_config.py.
# For tries, GCE or local can use the default jupyterhub_config.py
function append-to-jupyterhub-config {
  # Adding this to prevent the Hub container to crash loop.
  # Without the rm, the crash loop still happens.
  proxy_file=/jupyterhub-proxy.pid
  if test -f "$proxy_file"; then
    pid=$(cat ${proxy_file})
    kill -9 "${pid}"
    rm ${proxy_file}
    echo "Just killed ${pid}."
  fi

  # Checks if runs on AI Notebook by reading a metadata passed as an environment variable.
  jupyterhub_host_type=$( curl ${metadata_base_url}/instance/attributes/jupyterhub-host-type -H "Metadata-Flavor: Google" )
  if [ "$jupyterhub_host_type" == "ain" ]; then
    echo "Running on AI Notebook."

    # Sets default project and regions because JupyterHub is hosted on GCP.
    export PROJECT
    PROJECT=$( curl -s ${metadata_base_url}/project/project-id -H "Metadata-Flavor: Google")
    set-environment-from-metadata
    set-region-and-zone-from-metadata
    set-default-subnet
    set-default-configs
    set-default-name-pattern

    append-config-ain
    return 0
  fi

  echo "Running somewhere that is not an AI Notebook."
  append-config-testing
  return 0
}

# 'set-environment-from-metadata'
# Sets relevant Dataproc Hub environment variables from instance metadata
# Note that these will override settings from container-env-file
function set-environment-from-metadata {
  local notebooks_location
  local dataproc_configs
  local dataproc_locations
  local default_subnet
  local default_service_account
  local allow_custom_clusters
  local machine_types_list
  local notebooks_examples_location
  local hub_allow_named_servers

  notebooks_location=$( curl -s -f \
    ${metadata_base_url}/instance/attributes/notebooks-location -H "Metadata-Flavor: Google" || echo )
  dataproc_configs=$( curl -s -f \
    ${metadata_base_url}/instance/attributes/dataproc-configs -H "Metadata-Flavor: Google" || echo )
  dataproc_locations=$( curl -s -f \
    ${metadata_base_url}/instance/attributes/dataproc-locations-list -H "Metadata-Flavor: Google" || echo )
  default_subnet=$( curl -s -f \
    ${metadata_base_url}/instance/attributes/dataproc-default-subnet -H "Metadata-Flavor: Google" || echo )
  default_service_account=$( curl -s -f \
    ${metadata_base_url}/instance/attributes/dataproc-service-account -H "Metadata-Flavor: Google" || echo )
  allow_custom_clusters=$( curl -s -f \
    ${metadata_base_url}/instance/attributes/dataproc-allow-custom-clusters -H "Metadata-Flavor: Google" || echo )
  machine_types_list=$( curl -s -f \
    ${metadata_base_url}/instance/attributes/dataproc-machine-types-list -H "Metadata-Flavor: Google" || echo )
  notebooks_examples_location=$( curl -s -f \
    ${metadata_base_url}/instance/attributes/notebooks-examples-location -H "Metadata-Flavor: Google" || echo )
  hub_allow_named_servers=$( curl -s -f \
    ${metadata_base_url}/instance/attributes/hub-allow-named-servers -H "Metadata-Flavor: Google" || echo )

  if [ -n "${notebooks_location}" ]; then
    export NOTEBOOKS_LOCATION="${notebooks_location}"
  fi
  if [ -n "${dataproc_configs}" ]; then
    export DATAPROC_CONFIGS="${dataproc_configs}"
  fi
  if [ -n "${dataproc_locations}" ]; then
    export DATAPROC_LOCATIONS_LIST="${dataproc_locations}"
  fi
  if [ -n "${default_subnet}" ]; then
    export DATAPROC_DEFAULT_SUBNET="${default_subnet}"
  fi
  if [ -n "${default_service_account}" ]; then
    export DATAPROC_SERVICE_ACCOUNT="${default_service_account}"
  fi
  if [ -n "${allow_custom_clusters}" ]; then
    export DATAPROC_ALLOW_CUSTOM_CLUSTERS="${allow_custom_clusters}"
  fi
  if [ -n "${machine_types_list}" ]; then
    export DATAPROC_MACHINE_TYPES_LIST="${machine_types_list}"
  fi
  if [ -n "${notebooks_examples_location}" ]; then
    export NOTEBOOKS_EXAMPLES_LOCATION="${notebooks_examples_location}"
  fi
  if [ -n "${hub_allow_named_servers}" ]; then
    export HUB_ALLOW_NAMED_SERVERS="${hub_allow_named_servers}"
  fi
}

# 'set-region-and-zone-from-metadata'
# If region environment variable is not set, then infer from
# the region part of the zone obtained from instance metadata.
# If the DATAPROC_LOCATIONS_LIST environment variable is not set,
# then infer a valid suffix from the current zone
function set-region-and-zone-from-metadata {
  zone_uri=$( curl -s ${metadata_base_url}/instance/zone -H "Metadata-Flavor: Google")
  region=$( echo $zone_uri | sed -En 's:^projects/.+/zones/([a-z]+-[a-z]+[0-9]+)-([a-z])$:\1:p' )
  zone_suffix=$( echo $zone_uri | sed -En 's:^projects/.+/zones/([a-z]+-[a-z]+[0-9]+)-([a-z])$:\2:p' )
  if [ -z "$JUPYTERHUB_REGION" ];
  then
    export JUPYTERHUB_REGION="${region}"
  fi
  if [ -z "$DATAPROC_LOCATIONS_LIST" ];
  then
    echo "DATAPROC_LOCATIONS_LIST not specified, using Hub instance zone suffix -${zone_suffix}"
    export DATAPROC_LOCATIONS_LIST="${zone_suffix}"
  fi
}


# 'set-default-subnet'
# Set the DATAPROC_DEFAULT_SUBNET environment variable
# Call this function only after JUPYTERHUB_REGION has been set
# Multiple fallbacks for backwards compatibility if users don't specify a subnet
# 1. If the subnet is explicitly set by the user, respect that
# 2. If the instance subnet is specified in metadata (by the UI), use that
# 3. Attempt to call Compute API to determine which subnet this instance is in
# 4. If all else fails, set the default subnet to 'default' for the region
#    that the instance is in and hope for the best
function set-default-subnet() {
  # Respect default subnet if explicitly set
  if [ ! -z "${DATAPROC_DEFAULT_SUBNET}" ];
  then
    return 0
  fi

  # If subnet set in metadata, use that
  echo "No Dataproc default subnet specified in environment, trying to get subnet URI from instance metadata"
  local metadata_subnet
  metadata_subnet=$( curl -s --fail ${metadata_base_url}/instance/attributes/instance-subnet-uri -H "Metadata-Flavor: Google" )
  local metadata_call_ret=$?
  if [ "${metadata_call_ret}" -eq "0" ];
  then
    echo "Inferred subnet from metadata: ${metadata_subnet}"
    export DATAPROC_DEFAULT_SUBNET="${metadata_subnet}"
    return 0
  fi

  # Attempt to call compute API to determine which subnet this instance is on
  echo "Couldn't determine subnet from metadata, trying to get subnet URI from compute API."
  local name
  local zone
  local compute_api_subnet
  name=$( curl -s ${metadata_base_url}/instance/name -H "Metadata-Flavor: Google" )
  zone=$( curl -s ${metadata_base_url}/instance/zone -H "Metadata-Flavor: Google" | \
                sed -En 's:^projects/.+/zones/([a-z]+-[a-z]+[0-9]+-[a-z])$:\1:p' )
  compute_api_subnet=$( gcloud compute instances describe "${name}" --zone="${zone}" --format='value[](networkInterfaces.subnetwork)' )
  local compute_api_ret=$?
  if [ "${compute_api_ret}" -eq "0" ];
  then
    echo "Inferred subnet by calling Compute API: ${compute_api_subnet}"
    export DATAPROC_DEFAULT_SUBNET="${compute_api_subnet}"
    return 0
  fi

  # As a last-ditch effort, assume the default subnet exists for this project
  echo "Couldn't get subnet from compute API, falling back to using 'default' subnet."
  local subnet="https://www.googleapis.com/compute/v1/projects/${PROJECT}/regions/${JUPYTERHUB_REGION}/subnetworks/default"
  echo "Setting subnet to ${subnet}"
  export DATAPROC_DEFAULT_SUBNET="${subnet}"
}


# 'set-default-configs'
# If the user didn't specify any relevant configs, point them torwards a fixed
# set of public example configs
function set-default-configs() {
  if [ -z "${DATAPROC_CONFIGS}" ];
  then
    echo "Dataproc configs not specified, falling back to public configs"
    export DATAPROC_CONFIGS="gs://dataproc-spawner-dist/example-configs/single-node-cluster.yaml,gs://dataproc-spawner-dist/example-configs/standard-cluster.yaml"
  fi
}


# 'set-default-name-pattern'
# If the user didn't specify a name pattern for clusters spawned by this Hub,
# assign one by default that makes the name distinct within a project
function set-default-name-pattern() {
  if [ -z "${CLUSTER_NAME_PATTERN}" ];
  then
    default_name_pattern="hub-$( curl -s ${metadata_base_url}/instance/name -H "Metadata-Flavor: Google" )-{}"
    echo "Default name pattern not specified. Setting default name pattern to ${default_name_pattern}"
    export CLUSTER_NAME_PATTERN="${default_name_pattern}"
  fi
}

append-to-jupyterhub-config

# Starts JupyterHub.
jupyterhub
