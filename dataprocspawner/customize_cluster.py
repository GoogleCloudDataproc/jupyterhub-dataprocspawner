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

"""Functions for creating form to customize cluster configuration."""


def get_base_cluster_html_form(configs, locations_list, jhub_region):
  """
    Args:
      - List configs: List of Cloud Dataproc cluster config files path in GCS.
      - List locations_list: List of zone letters to choose from to create a
        Cloud Dataproc cluster in the JupyterHub region. ex: ["a", "b"]
  """
  locations_list = [i for i in locations_list if i]
  configs = [i for i in configs if i]

  # Cluster configuration
  html_config = f"""
    <section class="form-section">
      <br />
      <div class="mdc-select jupyter-select mdc-select--outlined">
        <input
          type="hidden"
          name="cluster_type"
          value="{configs[0]}"/>
        <div class="mdc-select__anchor">
          <span class="mdc-select__selected-text"></span>
          <span class="mdc-select__dropdown-icon">
            <svg
              width="10px"
              height="5px"
              viewBox="7 10 10 5"
              focusable="false">
                <polygon
                  class="mdc-select__dropdown-icon-inactive"
                  stroke="none"
                  fill-rule="evenodd"
                  points="7 10 12 15 17 10">
                </polygon>
                <polygon
                  class="mdc-select__dropdown-icon-active"
                  stroke="none"
                  fill-rule="evenodd"
                  points="7 15 12 10 17 15">
                </polygon>
            </svg>
          </span>
          <span class="mdc-notched-outline">
            <span class="mdc-notched-outline__leading"></span>
            <span class="mdc-notched-outline__notch">
              <span class="mdc-floating-label">
                Cluster's configuration
              </span>
            </span>
            <span class="mdc-notched-outline__trailing"></span>
          </span>
        </div>
      <div class="mdc-select__menu mdc-menu mdc-menu-surface">
        <ul class="mdc-list">"""

  for config in configs:
    name = ".".join(config.split("/")[-1].split(".")[:-1])
    html_config += f"""
      <li class="mdc-list-item" data-value="{config}">
        <span class="mdc-list-item__text">{name}</span>
      </li>"""

  html_config += """
    \t\t</ul>
    \t</div>
    </div>
    <br />
    <br />"""

  # Zone configuration
  html_zone = f"""
    <div class="mdc-select jupyter-select mdc-select--outlined">
      <input
        type="hidden"
        name="cluster_zone"
        value="{jhub_region}-{locations_list[0]}"/>
      <div class="mdc-select__anchor">
        <span class="mdc-select__selected-text"></span>
        <span class="mdc-select__dropdown-icon">
          <svg
            width="10px"
            height="5px"
            viewBox="7 10 10 5"
            focusable="false">
              <polygon
                class="mdc-select__dropdown-icon-inactive"
                stroke="none"
                fill-rule="evenodd"
                points="7 10 12 15 17 10">
              </polygon>
              <polygon
                class="mdc-select__dropdown-icon-active"
                stroke="none"
                fill-rule="evenodd"
                points="7 15 12 10 17 15">
              </polygon>
          </svg>
        </span>
        <span class="mdc-notched-outline">
          <span class="mdc-notched-outline__leading"></span>
          <span class="mdc-notched-outline__notch">
            <span class="mdc-floating-label">
              Zone
            </span>
          </span>
          <span class="mdc-notched-outline__trailing"></span>
        </span>
      </div>
    <div class="mdc-select__menu mdc-menu mdc-menu-surface">
      <ul class="mdc-list">"""

  for zone_letter in locations_list:
    location = f"{jhub_region}-{zone_letter}"
    html_zone += f"""
      <li class="mdc-list-item" data-value="{location}">
        <span class="mdc-list-item__text">{location}</span>
      </li>"""

  html_zone += """
    \t\t\t</ul>
    \t\t</div>
    \t</div>
    </section>"""

  return html_config + "\n" + html_zone


def get_custom_cluster_html_form(autoscaling_policies, node_types):
  autoscaling_policies = [i for i in autoscaling_policies if i]
  node_types = [i for i in node_types if i]

  html_toggler_close = """
    </section>
    </div>
    </div>"""

  html_toggler = """
    <div class="jupyter-cluster-customization-zone">
      <div class="jupyter-cluster-customization-zone__top">
        <button
          id="jupyter-cluster-customization-zone-toggler"
          class="mdc-button"
          aria-pressed="false">
            <div class="mdc-button__ripple"></div>
            <i class="material-icons mdc-button__icon" aria-hidden="true">add</i>
            <span class="mdc-button__label">Customize your cluster</span>
        </button>
      </div>
      <div
        id="jupyter-cluster-customization-zone-wrapper"
        class="jupyter-cluster-customization-zone__content"
        hidden>
          <section class="form-section">
            <input
              id="custom-cluster"
              name="custom_cluster"
              value="" type="hidden"/>"""

  # Autoscaling policies configuration
  html_autoscaling_policy = ""
  if autoscaling_policies:
    html_autoscaling_policy = """
      <div class="mdc-select jupyter-select mdc-select--outlined">
        <input
          type="hidden"
          name="autoscaling_policy"
          value="None"/>
        <div class="mdc-select__anchor">
          <span class="mdc-select__selected-text"></span>
          <span class="mdc-select__dropdown-icon">
            <svg
              width="10px"
              height="5px"
              viewBox="7 10 10 5"
              focusable="false">
                <polygon
                  class="mdc-select__dropdown-icon-inactive"
                  stroke="none"
                  fill-rule="evenodd"
                  points="7 10 12 15 17 10">
                </polygon>
                <polygon
                  class="mdc-select__dropdown-icon-active"
                  stroke="none"
                  fill-rule="evenodd"
                  points="7 15 12 10 17 15">
                </polygon>
            </svg>
          </span>
          <span class="mdc-notched-outline">
            <span class="mdc-notched-outline__leading"></span>
            <span class="mdc-notched-outline__notch">
              <span class="mdc-floating-label">
                Autoscaling policies
              </span>
            </span>
            <span class="mdc-notched-outline__trailing"></span>
          </span>
        </div>
        <div class="mdc-select__menu mdc-menu mdc-menu-surface">
          <ul class="mdc-list">
            <li class="mdc-list-item" data-value="None">
              <span class="mdc-list-item__text">None</span>
            </li>"""

  for policy in autoscaling_policies:
    html_autoscaling_policy += f"""
      <li class="mdc-list-item" data-value="{policy}">
        <span class="mdc-list-item__text">{policy}</span>
      </li>"""

  html_autoscaling_policy += """
    \t\t</ul>
    \t</div>
    </div>
    <br />
    <br />"""

  html_pip_packages = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="pip_packages">
            Install the following pip packages
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="pip_packages"
        aria-labelledby="pip_packages"/>
    </label>
    <div class="mdc-text-field-helper-line">
      <div class="mdc-text-field-helper-text" aria-hidden="true">
        Example: 'pandas==0.23.0 scipy==1.1.0'
      </div>
    </div>
    <br />"""

  html_conda_packages = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="conda_packages">
            Install the following conda packages
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="conda_packages"
        aria-labelledby="conda_packages"/>
    </label>
    <div class="mdc-text-field-helper-line">
      <div class="mdc-text-field-helper-text" aria-hidden="true">
        Example: 'scipy=1.1.0 tensorflow'
      </div>
    </div>
    <br />"""

  html_cluster_image = """
    <h2 class="jupyter-form__group-title">Versioning</h2>
    <div
      class="mdc-radio-decision-group"
      data-name="image_version"
      data-selected-value="custom">
        <div class="mdc-form-field mdc-form-field-full mdc-form-field-radio">
          <div class="mdc-radio">
            <input
              class="mdc-radio__native-control"
              type="radio"
              id="image-radio1"
              name="image_version"
              value="preview-debian10"/>
            <div class="mdc-radio__background">
              <div class="mdc-radio__outer-circle"></div>
              <div class="mdc-radio__inner-circle"></div>
            </div>
            <div class="mdc-radio__ripple"></div>
          </div>
          <label for="image-radio1">PREVIEW 2.0</label>
        </div>
        <br />
        <div class="mdc-form-field mdc-form-field-full mdc-form-field-radio">
          <div class="mdc-radio">
            <input
              class="mdc-radio__native-control"
              type="radio"
              id="image-radio2"
              name="image_version"
              value="1.5-debian10"
              checked/>
            <div class="mdc-radio__background">
              <div class="mdc-radio__outer-circle"></div>
              <div class="mdc-radio__inner-circle"></div>
            </div>
            <div class="mdc-radio__ripple"></div>
          </div>
          <label for="image-radio2">1.5</label>
        </div>
        <br />
        <div class="mdc-form-field mdc-form-field-full mdc-form-field-radio">
          <div class="mdc-radio">
            <input
              class="mdc-radio__native-control"
              type="radio"
              id="image-radio3"
              name="image_version"
              value="1.4-debian10"/>
            <div class="mdc-radio__background">
              <div class="mdc-radio__outer-circle"></div>
              <div class="mdc-radio__inner-circle"></div>
            </div>
            <div class="mdc-radio__ripple"></div>
          </div>
          <label for="image-radio3">1.4</label>
        </div>
        <br />
        <div class="mdc-form-field mdc-form-field-full mdc-form-field-radio">
          <div class="mdc-radio">
            <input
              class="mdc-radio__native-control"
              type="radio"
              id="image-radio4"
              name="image_version"
              value="1.3-debian10"/>
            <div class="mdc-radio__background">
              <div class="mdc-radio__outer-circle"></div>
              <div class="mdc-radio__inner-circle"></div>
            </div>
            <div class="mdc-radio__ripple"></div>
          </div>
          <label for="image-radio4">1.3</label>
        </div>
        <br />
        <div class="mdc-form-field mdc-form-field-full mdc-form-field-radio">
          <div class="mdc-radio">
            <input
              class="mdc-radio__native-control"
              type="radio"
              id="image-radio5"
              name="image_version"
              value="custom"/>
            <div class="mdc-radio__background">
              <div class="mdc-radio__outer-circle"></div>
              <div class="mdc-radio__inner-circle"></div>
            </div>
            <div class="mdc-radio__ripple"></div>
          </div>
          <label for="image-radio5">Custom image</label>
        </div>
        <br />
        <br />
        <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined
          mdc-radio-decision-group-target">
            <span class="mdc-notched-outline">
              <span class="mdc-notched-outline__leading"></span>
              <span class="mdc-notched-outline__notch">
                <span class="mdc-floating-label">
                  Custom image
                </span>
              </span>
              <span class="mdc-notched-outline__trailing"></span>
            </span>
            <input
              type="text"
              class="mdc-text-field__input"
              name="custom_image"
              id="custom_image"/>
        </label>
        <div class="mdc-text-field-helper-line">
          <div class="mdc-text-field-helper-text" aria-hidden="true">
            Example: 'projects/project-id/global/images/image-id'
          </div>
        </div>
    </div>
    <br />"""

  html_master_base = """
    <h2 class="jupyter-form__group-title">Master node</h2>
    <br />"""

  html_master_type = """
    <div class="mdc-select jupyter-select mdc-select--outlined">
      <input
        type="hidden"
        name="master_node_type"
        value="n1-standard-4">
      <div class="mdc-select__anchor">
        <span class="mdc-select__selected-text"></span>
        <span class="mdc-select__dropdown-icon">
          <svg
            width="10px"
            height="5px"
            viewBox="7 10 10 5"
            focusable="false">
              <polygon
                class="mdc-select__dropdown-icon-inactive"
                stroke="none"
                fill-rule="evenodd"
                points="7 10 12 15 17 10">
              </polygon>
              <polygon
                class="mdc-select__dropdown-icon-active"
                stroke="none"
                fill-rule="evenodd"
                points="7 15 12 10 17 15">
              </polygon>
          </svg>
        </span>
        <span class="mdc-notched-outline">
          <span class="mdc-notched-outline__leading"></span>
          <span class="mdc-notched-outline__notch">
            <span class="mdc-floating-label">
              Machine type
            </span>
          </span>
          <span class="mdc-notched-outline__trailing"></span>
        </span>
      </div>
      <div class="mdc-select__menu mdc-menu mdc-menu-surface">
        <ul class="mdc-list">
          <li class="mdc-list-item" data-value="n1-standard-4">
            <span class="mdc-list-item__text">n1-standard-4 (default)</span>
          </li>"""

  for node_type in node_types:
    html_master_type += f"""
      <li class="mdc-list-item" data-value="{node_type}">
        <span class="mdc-list-item__text">{node_type}</span>
      </li>"""

  html_master_type += """
    \t\t</ul>
    \t</div>
    </div>
    <br />
    <br />"""

  html_master_disk_type = """
    <div class="mdc-select jupyter-select mdc-select--outlined">
      <input
        type="hidden"
        name="master_disk_type"
        value="pd-standard"/>
      <div class="mdc-select__anchor">
        <span class="mdc-select__selected-text"></span>
        <span class="mdc-select__dropdown-icon">
          <svg
            width="10px"
            height="5px"
            viewBox="7 10 10 5"
            focusable="false">
              <polygon
                class="mdc-select__dropdown-icon-inactive"
                stroke="none"
                fill-rule="evenodd"
                points="7 10 12 15 17 10">
              </polygon>
              <polygon
                class="mdc-select__dropdown-icon-active"
                stroke="none"
                fill-rule="evenodd"
                points="7 15 12 10 17 15">
              </polygon>
          </svg>
        </span>
        <span class="mdc-notched-outline">
          <span class="mdc-notched-outline__leading"></span>
          <span class="mdc-notched-outline__notch">
            <span class="mdc-floating-label">Primary disk type</span>
          </span>
          <span class="mdc-notched-outline__trailing"></span>
        </span>
      </div>
      <div class="mdc-select__menu mdc-menu mdc-menu-surface">
        <ul class="mdc-list">
          <li class="mdc-list-item" data-value="pd-standard">
            <span class="mdc-list-item__text">Standard Persistent Disk</span>
          </li>
          <li class="mdc-list-item" data-value="pd-ssd">
            <span class="mdc-list-item__text">SSD Persistent Disk</span>
          </li>
        </ul>
      </div>
    </div>
    <br />
    <br />"""

  html_master_disk_size = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="master_disk_size">
            Master disk size
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="master_disk_size"
        aria-labelledby="master_disk_size"
        value="500"/>
    </label>
    <br />
    <br />"""

  html_worker_base = """
    <h2 class="jupyter-form__group-title">Worker nodes</h2>
    <br />"""

  html_worker_type = """
    <div class="mdc-select jupyter-select mdc-select--outlined">
      <input
        type="hidden"
        name="worker_node_type"
        value="n1-standard-4"/>
      <div class="mdc-select__anchor">
        <span class="mdc-select__selected-text"></span>
        <span class="mdc-select__dropdown-icon">
          <svg
            width="10px"
            height="5px"
            viewBox="7 10 10 5"
            focusable="false">
              <polygon
                class="mdc-select__dropdown-icon-inactive"
                stroke="none"
                fill-rule="evenodd"
                points="7 10 12 15 17 10">
              </polygon>
              <polygon
                class="mdc-select__dropdown-icon-active"
                stroke="none"
                fill-rule="evenodd"
                points="7 15 12 10 17 15">
              </polygon>
          </svg>
        </span>
        <span class="mdc-notched-outline">
          <span class="mdc-notched-outline__leading"></span>
          <span class="mdc-notched-outline__notch">
            <span class="mdc-floating-label">
              Machine type
            </span>
          </span>
          <span class="mdc-notched-outline__trailing"></span>
        </span>
      </div>
      <div class="mdc-select__menu mdc-menu mdc-menu-surface">
        <ul class="mdc-list">
          <li class="mdc-list-item" data-value="n1-standard-4">
            <span class="mdc-list-item__text">n1-standard-4 (default)</span>
          </li>"""

  for node_type in node_types:
    html_worker_type += f"""
      <li class="mdc-list-item" data-value="{node_type}">
        <span class="mdc-list-item__text">{node_type}</span>
      </li>"""

  html_worker_type += """
    \t\t</ul>
    \t</div>
    </div>
    <br />
    <br />"""

  html_worker_disk_type = """
    <div class="mdc-select jupyter-select mdc-select--outlined">
      <input
        type="hidden"
        name="worker_disk_type"
        value="pd-standard"/>
      <div class="mdc-select__anchor">
        <span class="mdc-select__selected-text">pd-standard</span>
        <span class="mdc-select__dropdown-icon">
          <svg
            width="10px"
            height="5px"
            viewBox="7 10 10 5"
            focusable="false">
              <polygon
                class="mdc-select__dropdown-icon-inactive"
                stroke="none"
                fill-rule="evenodd"
                points="7 10 12 15 17 10">
              </polygon>
              <polygon
                class="mdc-select__dropdown-icon-active"
                stroke="none"
                fill-rule="evenodd"
                points="7 15 12 10 17 15">
              </polygon>
          </svg>
        </span>
        <span class="mdc-notched-outline">
          <span class="mdc-notched-outline__leading"></span>
          <span class="mdc-notched-outline__notch">
            <span class="mdc-floating-label">Primary disk type</span>
          </span>
          <span class="mdc-notched-outline__trailing"></span>
        </span>
      </div>
      <div class="mdc-select__menu mdc-menu mdc-menu-surface">
        <ul class="mdc-list">
          <li class="mdc-list-item" data-value="pd-standard">
            <span class="mdc-list-item__text">Standard Persistent Disk</span>
          </li>
          <li class="mdc-list-item" data-value="pd-ssd">
            <span class="mdc-list-item__text">SSD Persistent Disk</span>
          </li>
        </ul>
      </div>
    </div>
    <br />
    <br />"""

  html_worker_disk_size = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="worker_disk_size">
            Primary disk size
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="worker_disk_size"
        aria-labelledby="worker_disk_size"
        value="500"/>
    </label>
    <br />
    <br />"""

  html_worker_amount = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="worker_node_amount">
            Number of worker nodes
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="worker_node_amount"
        aria-labelledby="worker_node_amount"
        value="2"/>
    </label>
    <br />
    <br />"""

  html_secondary_worker_base = """
    <h2 class="jupyter-form__group-title">Secondary worker nodes</h2>
    <br />"""

  html_secondary_worker_disk_type = """
    <div class="mdc-select jupyter-select mdc-select--outlined">
      <input
        type="hidden"
        name="sec_worker_disk_type"
        value="pd-standard"/>
      <div class="mdc-select__anchor">
        <span class="mdc-select__selected-text">pd-standard</span>
        <span class="mdc-select__dropdown-icon">
          <svg
            width="10px"
            height="5px"
            viewBox="7 10 10 5"
            focusable="false">
              <polygon
                class="mdc-select__dropdown-icon-inactive"
                stroke="none"
                fill-rule="evenodd"
                points="7 10 12 15 17 10">
              </polygon>
              <polygon
                class="mdc-select__dropdown-icon-active"
                stroke="none"
                fill-rule="evenodd"
                points="7 15 12 10 17 15">
              </polygon>
          </svg>
        </span>
        <span class="mdc-notched-outline">
          <span class="mdc-notched-outline__leading"></span>
          <span class="mdc-notched-outline__notch">
            <span class="mdc-floating-label">Primary disk type</span>
          </span>
          <span class="mdc-notched-outline__trailing"></span>
        </span>
      </div>
      <div class="mdc-select__menu mdc-menu mdc-menu-surface">
        <ul class="mdc-list">
          <li class="mdc-list-item" data-value="pd-standard">
            <span class="mdc-list-item__text">Standard Persistent Disk</span>
          </li>
          <li class="mdc-list-item" data-value="pd-ssd">
            <span class="mdc-list-item__text">SSD Persistent Disk</span>
          </li>
        </ul>
      </div>
    </div>
    <br />
    <br />"""

  html_secondary_worker_disk_size = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="sec_worker_disk_size">
            Primary disk size
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="sec_worker_disk_size"
        aria-labelledby="sec_worker_disk_size"
        value="500"/>
    </label>
    <br />
    <br />"""

  html_secondary_worker_amount = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="sec_worker_node_amount">
            Number of secondary worker nodes
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="sec_worker_node_amount"
        aria-labelledby="sec_worker_node_amount"
        value="0"/>
    </label>
    <br />
    <br />"""

  html_adv_base = """
    <h2 class="jupyter-form__group-title">Advanced</h2>
    <br />"""

  html_custom_labels = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="custom_labels">
            User defined labels
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="custom_labels"
        aria-labelledby="custom_labels"/>
    </label>
    <div class="mdc-text-field-helper-line">
      <div class="mdc-text-field-helper-text" aria-hidden="true">
        Example: 'key1:value1,key2:value2'
      </div>
    </div>
    <br />"""

  html_init_actions = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="init_actions">
            Initialization actions
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="init_actions"
        aria-labelledby="init_actions"/>
    </label>
    <div class="mdc-text-field-helper-line">
      <div class="mdc-text-field-helper-text" aria-hidden="true">
        Example: 'gs://init-action1,gs://init-action2'
      </div>
    </div>
    <br />"""

  html_hive_settings = """
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="hive_host">
            Hive metastore host
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="hive_host"
        aria-labelledby="hive_host"/>
    </label>
    <br />
    <br />
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="hive_db">
            Hive metastore database
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="hive_db"
        aria-labelledby="hive_db"/>
    </label>
    <br />
    <br />
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="hive_user">
            Hive metastore user name
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="hive_user"
        aria-labelledby="hive_user"/>
    </label>
    <br />
    <br />
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="hive_passwd">
            Hive metastore password
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="hive_passwd"
        aria-labelledby="hive_passwd"/>
    </label>
    <br />"""

  body = "\n".join([
    html_autoscaling_policy,
    html_pip_packages,
    html_conda_packages,
    html_cluster_image,
    html_master_base,
    html_master_type,
    html_master_disk_type,
    html_master_disk_size,
    html_worker_base,
    html_worker_type,
    html_worker_disk_type,
    html_worker_disk_size,
    html_worker_amount,
    html_secondary_worker_base,
    html_secondary_worker_disk_type,
    html_secondary_worker_disk_size,
    html_secondary_worker_amount,
    html_adv_base,
    html_custom_labels,
    html_init_actions,
    html_hive_settings
  ])

  return html_toggler + body + html_toggler_close
