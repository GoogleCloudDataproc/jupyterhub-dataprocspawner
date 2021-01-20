# Copyright 2020 Google LLC

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
      <div class="mdc-select mdc-select-decision jupyter-select mdc-select--outlined"
        data-decision-value="gs://dataproc-spawner-dist/example-configs/example-single-node.yaml"
        data-decision-target="#select-decision-target">
          <input
            type="hidden"
            name="cluster_type"
            value="{configs[0]}"/>"""

  html_config += _render_select_menu_part(
    label='Cluster configuration'
  )

  html_config += """
    <div class="mdc-select__menu mdc-menu mdc-menu-surface">
      <ul class="mdc-list">"""

  if configs:
    for config in configs:
      name = '.'.join(config.split('/')[-1].split('.')[:-1])
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
        value="{jhub_region}-{locations_list[0]}"/>"""

  html_zone += _render_select_menu_part(
    label='Zone'
  )

  html_zone += """
    <div class="mdc-select__menu mdc-menu mdc-menu-surface">
      <ul class="mdc-list">"""

  for zone_letter in locations_list:
    location = f'{jhub_region}-{zone_letter}'
    html_zone += f"""
      <li class="mdc-list-item" data-value="{location}">
        <span class="mdc-list-item__text">{location}</span>
      </li>"""

  html_zone += """
    \t\t\t</ul>
    \t\t</div>
    \t</div>
    </section>"""

  return html_config + '\n' + html_zone


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
  html_autoscaling_policy = """
    <div class="mdc-select jupyter-select mdc-select--outlined">
      <input
        type="hidden"
        name="autoscaling_policy"
        value="None"/>"""

  html_autoscaling_policy += _render_select_menu_part(
    label='Autoscaling policies'
  )

  html_autoscaling_policy += """
    <div class="mdc-select__menu mdc-menu mdc-menu-surface">
      <ul class="mdc-list">
        <li class="mdc-list-item" data-value="None">
          <span class="mdc-list-item__text">None</span>
        </li>"""

  if autoscaling_policies:
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

  html_pip_packages = _render_text_field(
    input_id='pip_packages',
    label='Install the following pip packages',
    hint='Example: \'pandas==0.23.0 scipy==1.1.0\''
  )

  html_conda_packages = _render_text_field(
    input_id='conda_packages',
    label='Install the following conda packages',
    hint='Example: \'scipy=1.1.0 tensorflow\''
  )

  html_cluster_image = """
    <h2 class="jupyter-form__group-title">Versioning</h2>
    <div
      class="mdc-radio-decision-group"
      data-name="image_version"
      data-selected-value="custom">"""

  html_cluster_image += _render_radio_btn(
    input_id='image-radio0',
    name='image_version',
    label='None',
    value='',
    checked='checked'
  )

  html_cluster_image += _render_radio_btn(
    input_id='image-radio1',
    name='image_version',
    label='PREVIEW 2.0',
    value='preview-debian10'
  )

  html_cluster_image += _render_radio_btn(
    input_id='image-radio2',
    name='image_version',
    label='1.5-debian10',
    value='1.5-debian10'
  )

  html_cluster_image += _render_radio_btn(
    input_id='image-radio3',
    name='image_version',
    label='1.4-debian10',
    value='1.4-debian10'
  )

  html_cluster_image += _render_radio_btn(
    input_id='image-radio4',
    name='image_version',
    label='Custom image',
    value='custom'
  )

  html_cluster_image += """
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
          id="custom_image"
          placeholder="Example: 'projects/project-id/global/images/image-id'"/>
    </label>
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
        value="n1-standard-4">"""

  html_master_type += _render_select_menu_part(
    label='Machine type'
  )

  html_master_type += """
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
        value="pd-standard"/>"""

  html_master_disk_type += _render_select_menu_part(
    label='Primary disk type'
  )

  html_master_disk_type += """
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

  html_master_disk_size = _render_text_field(
    input_id='master_disk_size',
    label='Primary disk size',
    value=500
  )

  html_desicion_target = """
    <div id="select-decision-target">"""

  html_worker_base = """
    <h2 class="jupyter-form__group-title">Worker nodes</h2>
    <br />"""

  html_worker_type = """
    <div class="mdc-select jupyter-select mdc-select--outlined">
      <input
        type="hidden"
        name="worker_node_type"
        value="n1-standard-4"/>"""

  html_worker_type += _render_select_menu_part(
    label='Machine type'
  )

  html_worker_type += """
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
        value="pd-standard"/>"""

  html_worker_disk_type += _render_select_menu_part(
    label='Primary disk type'
  )

  html_worker_disk_type += """
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

  html_worker_disk_size = _render_text_field(
    input_id='worker_disk_size',
    label='Primary disk size',
    value=500
  )

  html_worker_amount = _render_text_field(
    input_id='worker_node_amount',
    label='Number of worker nodes',
    value=2
  )

  html_secondary_worker_base = """
    <h2 class="jupyter-form__group-title">Secondary worker nodes</h2>
    <br />"""

  html_secondary_worker_disk_type = """
    <div class="mdc-select jupyter-select mdc-select--outlined">
      <input
        type="hidden"
        name="sec_worker_disk_type"
        value="pd-standard"/>"""

  html_secondary_worker_disk_type += _render_select_menu_part(
    label='Primary disk type'
  )

  html_secondary_worker_disk_type += """
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

  html_secondary_worker_disk_size = _render_text_field(
    input_id='sec_worker_disk_size',
    label='Primary disk size',
    value=500
  )

  html_secondary_worker_amount = _render_text_field(
    input_id='sec_worker_node_amount',
    label='Number of secondary worker nodes'
  )

  html_desicion_target_close = """
    </div>"""

  html_adv_base = """
    <h2 class="jupyter-form__group-title">Advanced</h2>
    <br />"""

  html_custom_labels = _render_text_field(
    input_id='custom_labels',
    label='User defined labels',
    hint='Example: \'key1:value1,key2:value2\''
  )

  html_cluster_props = """
    <div
      class="jupyter-cluster-generic-fields"
      data-generic-name="cluster_props_"
      data-generic-values="prefix_:Prefix label,key_:Key,val_:Value">
        <h2 class="jupyter-form__group-title">Cluster properties</h2>
        <br />
        <div class="jupyter-cluster-generic-fields-container"></div>
        <br />
        <div class="jupyter-cluster-generic-fields-bottom">
          <button class="mdc-button jupyter-cluster-generic-fields-add">
            <div class="mdc-button__ripple"></div>
            <i class="material-icons mdc-button__icon" aria-hidden="true">add</i>
            <span class="mdc-button__label">Add property</span>
          </button>
        </div>
    </div>
    <br />"""

  html_init_actions = _render_text_field(
    input_id='init_actions',
    label='Initialization actions',
    hint='Example: \'gs://init-action-1,gs://init-action-2\''
  )

  html_hive_settings_base = """
    <h2 class="jupyter-form__group-title">Hive settings</h2>
    <br />"""

  html_hive_host = _render_text_field(
    input_id='hive_host',
    label='Hive metastore host'
  )

  html_hive_database = _render_text_field(
    input_id='hive_db',
    label='Hive metastore database'
  )

  html_hive_username = _render_text_field(
    input_id='hive_user',
    label='Hive metastore user name'
  )

  html_hive_password = _render_text_field(
    input_id='hive_passwd',
    label='Hive metastore password'
  )

  body = '\n'.join([
    html_cluster_image,
    html_master_base,
    html_master_type,
    html_master_disk_type,
    html_master_disk_size,
    html_desicion_target,
    html_worker_base,
    html_worker_type,
    html_worker_disk_type,
    html_worker_disk_size,
    html_worker_amount,
    html_secondary_worker_base,
    html_secondary_worker_disk_type,
    html_secondary_worker_disk_size,
    html_secondary_worker_amount,
    html_desicion_target_close,
    html_adv_base,
    html_autoscaling_policy,
    html_pip_packages,
    html_conda_packages,
    html_custom_labels,
    html_init_actions,
    html_hive_settings_base,
    html_hive_host,
    html_hive_database,
    html_hive_username,
    html_hive_password,
    html_cluster_props
  ])

  return html_toggler + body + html_toggler_close

def _render_text_field(input_id, label, hint='', value=''):
  code = f"""
    <label class="mdc-text-field jupyter-text-field mdc-text-field--outlined">
      <span class="mdc-notched-outline">
        <span class="mdc-notched-outline__leading"></span>
        <span class="mdc-notched-outline__notch">
          <span class="mdc-floating-label" id="{input_id}">
            {label}
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
      <input
        type="text"
        class="mdc-text-field__input"
        name="{input_id}"
        placeholder="{hint}"
        value="{value}"/>
    </label>
    <br />
    <br />"""

  return code

def _render_radio_btn(input_id, label, name, value, checked=''):
  code = f"""
    <div class="mdc-form-field mdc-form-field-full mdc-form-field-radio">
      <div class="mdc-radio">
        <input
          class="mdc-radio__native-control"
          type="radio"
          id="{input_id}"
          name="{name}"
          value="{value}"
          {checked}/>
        <div class="mdc-radio__background">
          <div class="mdc-radio__outer-circle"></div>
          <div class="mdc-radio__inner-circle"></div>
        </div>
        <div class="mdc-radio__ripple"></div>
      </div>
      <label for="image-radio1">{label}</label>
    </div>
    <br />"""

  return code

def _render_select_menu_part(label):
  code = f"""
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
            {label}
          </span>
        </span>
        <span class="mdc-notched-outline__trailing"></span>
      </span>
    </div>"""

  return code
