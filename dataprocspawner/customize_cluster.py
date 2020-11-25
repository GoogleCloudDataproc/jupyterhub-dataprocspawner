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

  html_config = """
    <section class="form-section">
      <div class="form-group">
        <label for="cluster_type">Cluster's configuration</label>
        <select class="form-control" name="cluster_type">"""
  for config in configs:
    name = ".".join(config.split("/")[-1].split(".")[:-1])
    html_config += f'\n\t<option value="{config}">{name}</option>'
  html_config += """
    \t\t</select>
    \t</div>"""

  html_zone = """
    <div class="form-group">
      <label for="cluster_zone">Zone</label>
      <select class="form-control" name="cluster_zone">"""
  for zone in locations_list:
    location = f"{jhub_region}-{zone}"
    html_zone += f'\n\t<option value="{location}">{location}</option>'

  html_zone += """
    \t\t</select>
    \t</div>
    </section>"""

  return html_config + "\n" + html_zone


def get_custom_cluster_html_form(autoscaling_policies, node_types):
  autoscaling_policies = [i for i in autoscaling_policies if i]
  node_types = [i for i in node_types if i]

  head_html = """
    <a href="javascript:void(0);" id="customizationBtn">
      <i class="fa fa-angle-double-down" aria-hidden="true"></i>
      Customize your cluster
    </a>
    <div id="formcustomization" class="form-section" style="display: none;">
      <input id="is_custom_cluster" name="custom_cluster" value='' type="hidden">"""

  bottom_html = """
    </div>"""

  js_code = """
    <script>
      $("#customizationBtn").click(function () {
      $btn = $(this);
      $("#is_custom_cluster").val('1');
      $content = $("#formcustomization")
      $content.toggle()
      $("i:first-child").toggleClass('fa-angle-double-down');
      $("i:first-child").toggleClass('fa-angle-double-up');
      });
    </script>"""

  html_autoscaling_policy = ""
  if autoscaling_policies:
    html_autoscaling_policy += """
      <div class="form-group">
        <label for="autoscaling_policy">Autoscaling Policy</label>
        <select name="autoscaling_policy" class="form-control">
          <option value=''>None</option>"""
    for i in autoscaling_policies:
      html_autoscaling_policy += f'\n\t<option value="{i}">{i}</option>'
    html_autoscaling_policy += """
      \t</select>
      </div>"""

  html_pip_packages = """
    <div class="form-group">
      <label for="pip_packages">Install the following pip packages</label>
      <input name="pip_packages" class="form-control" value=''
        placeholder="Example: 'pandas==0.23.0 scipy==1.1.0'">
      </input>
    </div>"""

  html_condo_packages = """
    <div class="form-group">
      <label for="condo_packages">Install the following condo packages</label>
      <input name="condo_packages" class="form-control" value=''
        placeholder="Example: 'scipy=1.1.0 tensorflow'">
      </input>
    </div>"""

  html_cluster_image = """
    <br />
    <div class="form-group" name="versioning">
      <label for="versioning"><h1>Versioning</h1></label>
      <div>
        <input onclick="document.getElementById('custom_image').disabled = true;"
          type="radio" id="image-radio1" name="image_version" value="preview-debian10">
        <label for="customRadio1">PREVIEW 2.0</label>
      </div>
      <div>
        <input onclick="document.getElementById('custom_image').disabled = true;"
          type="radio" id="image-radio2" name="image_version" value="1.5-debian10" checked>
        <label for="customRadio2">1.5</label>
      </div>
      <div>
        <input onclick="document.getElementById('custom_image').disabled = true;"
          type="radio" id="image-radio3" name="image_version" value="1.4-debian10">
        <label for="customRadio3">1.4</label>
      </div>
      <div>
        <input onclick="document.getElementById('custom_image').disabled = true;"
          type="radio" id="image-radio4" name="image_version" value="1.3-debian10">
        <label for="customRadio4">1.3</label>
      </div>
      <div>
        <input onclick="document.getElementById('custom_image').disabled = false;"
          type="radio" id="image-radio5" name="image_version">
        <label for="customRadio5">Custom image</label>
        <input class="form-control" id="custom_image" name="custom_image"
          placeholder="projects/<project_id>/global/images/<image_id>" disabled>
      </div>
    </div>"""

  html_master_type = """
    <br />
    <div class="form-group" name="master">
      <label for="master"><h1>Master node</h1></label>"""
  if node_types:
    html_master_type += """
      <div class="form-group">
        <label for="master_node_type">Machine type</label>
        <select class="form-control" name="master_node_type">
          <option value=''>Default</option>"""
    for t in node_types:
      html_master_type += f'\n\t<option value="{t}">{t}</option>'
    html_master_type += """
      \t</select>
      </div>"""

  html_master_disk_type = """
    <div class="form-group">
      <label for="master_disk_type">Primary disk type</label>
      <select class="form-control" name="master_disk_type">
        <option value="pd-standard">Standard Persistent Disk</option>
        <option value="pd-ssd">SSD Persistent Disk</option>
      </select>
    </div>"""

  html_master_disk_size = """
    <div class="form-group">
      <label for="master_disk_size">Primary disk size</label>
      <input name="master_disk_size" class="form-control" placeholder="default" value=''></input>
    </div>
    </div>"""

  html_worker_type = """
    <br />
    <div class="form-group" name="worker">
      <label for="worker"><h1>Worker nodes</h1></label>"""
  if node_types:
    html_worker_type += """
      <div class="form-group">
        <label for="worker_node_type">Machine type</label>
        <select class="form-control" name="worker_node_type">
          <option value=''>Default</option>"""
    for t in node_types:
      html_worker_type += f'\n\t<option value="{t}">{t}</option>'
    html_worker_type += """
      \t</select>
      </div>"""

  html_worker_disk_type = """
    <div class="form-group">
      <label for="worker_disk_type">Primary disk type</label>
      <select class="form-control" name="worker_disk_type">
        <option value="pd-standard">Standard Persistent Disk</option>
        <option value="pd-ssd">SSD Persistent Disk</option>
      </select>
    </div>"""

  html_worker_disk_size = """
    <div class="form-group">
      <label for="worker_disk_size">Primary disk size</label>
      <input name="worker_disk_size" class="form-control" placeholder="default" value=''></input>
    </div>"""

  html_worker_amount = """
    <div class="form-group">
      <label for="worker_node_amount">Number of worker nodes</label>
      <input name="worker_node_amount" class="form-control" placeholder="" value='2'></input>
    </div>
    </div>"""

  html_secondary_worker_preemptibility = """
    <div class="form-group">
      <label for="sec_worker_preempt">Preemptibility</label>
      <select class="form-control" name="sec_worker_preempt">
        <option value="PREEMPTIBLE">Preemptible</option>
        <option value="NON-PREEMPTIBLE">Non-preemptible</option>
      </select>
    </div>"""

  html_secondary_worker_disk_type = """
    <br />
    <div class="form-group" name="sec_worker">
      <label for="sec_worker"><h1>Secondary worker nodes</h1></label>
      <div class="form-group">
        <label for="sec_worker_disk_type">Primary disk type</label>
        <select class="form-control" name="sec_worker_disk_type">
          <option value="pd-standard">Standard Persistent Disk</option>
          <option value="pd-ssd">SSD Persistent Disk</option>
        </select>
      </div>"""

  html_secondary_worker_disk_size = """
    <div class="form-group">
      <label for="sec_worker_disk_size">Primary disk size</label>
      <input name="sec_worker_disk_size" class="form-control"
        placeholder="default" value=''></input>
    </div>"""

  html_secondary_worker_amount = """
    <div class="form-group">
      <label for="sec_worker_node_amount">Number of secondary worker nodes</label>
      <input name="sec_worker_node_amount" class="form-control" placeholder="" value='0'></input>
    </div>
    </div>"""

  html_custom_labels = """
    <br />
    <div class="form-group">
      <label for="custom_labels">User defined labels</label>
      <input name="custom_labels" class="form-control" placeholder="key1:value1,key2:value2"
        value=''>
      </input>
    </div>"""

  html_init_actions = """
    <div class="form-group">
      <label for="init_actions">Initialization actions</label>
      <input name="init_actions" class="form-control"
        placeholder="gs://<init_action1>,gs://<init_action2>" value="">
      </input>
    </div>"""

  html_cluster_properties = """
    <div class="form-group">
      <label for="cluster_properties">Cluster properties</label>
      <input name="cluster_properties" class="form-control"
        placeholder="prefix:property1=value1,prefix:property2=value2" value="">
      </input>
    </div>"""

  html_hive_settings = """
    <div class="form-group">
      <label for="hive_host">Hive Metastore host</label>
      <input name="hive_host" class="form-control" placeholder='' value=''></input>
    </div>
    <div class="form-group">
      <label for="hive_db">Hive Metastore database</label>
      <input name="hive_db" class="form-control" placeholder='' value=''></input>
    </div>
    <div class="form-group">
      <label for="hive_user">Hive Metastore user name</label>
      <input name="hive_user" class="form-control" placeholder='' value=''></input>
    </div>
    <div class="form-group">
      <label for="hive_passwd">Hive Metastore password</label>
      <input name="hive_passwd" class="form-control" placeholder='' value=''></input>
    </div>"""

  body = "\n".join([
    html_autoscaling_policy,
    html_pip_packages,
    html_condo_packages,
    html_cluster_image,
    html_master_type,
    html_master_disk_type,
    html_master_disk_size,
    html_worker_type,
    html_worker_disk_type,
    html_worker_disk_size,
    html_worker_amount,
    # Temporary disabled
    # html_secondary_worker_preemptibility,
    html_secondary_worker_disk_type,
    html_secondary_worker_disk_size,
    html_secondary_worker_amount,
    html_custom_labels,
    html_init_actions,
    # html_cluster_properties,
    html_hive_settings
  ])

  return head_html + "\n" + js_code + "\n" + body + "\n" + bottom_html
