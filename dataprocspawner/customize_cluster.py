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


def get_base_cluster_html_form(configs, locations_list, jupyterhub_region):
  """
  Args:
    - List configs: List of Cloud Dataproc cluster config files path in GCS.
    - List locations_list: List of zones letters to choose from to create a
      Cloud Dataproc cluster in the JupyterHub region. ex: ["a", "b"]
  """
  locations_list = [i for i in locations_list if i]
  configs = [i for i in configs if i]

  html_config = '''
  <section class="form-section">
  <div class="form-group"> <label for="cluster_type">Cluster's configuration</label>
  <select class="form-control" name="cluster_type">\n'''
  for config in configs:
    name = ".".join(config.split("/")[-1].split(".")[:-1])
    html_config += f'''\t<option value="{config}">{name}</option>\n'''
  html_config += '''</select></div>'''

  html_zone = '''<div class="form-group">
  <label for="cluster_zone">Zone</label>
  <select class="form-control" name="cluster_zone">\n'''

  html_zone_options = []
  for zone_letter in locations_list:
    location = f"{jupyterhub_region}-{zone_letter}"
    html_zone_options.append(
        f'''\t<option value="{location}">{location}</option>\n''')

  html_zone += "".join(html_zone_options)
  html_zone += '''</select></div></section>'''

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
  <input id="is_custom_cluster" name="custom_cluster" value="" type="hidden">"""
  bottom_html = """</div>"""
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
  </script>
  """

  html_autoscaling_policy = ""
  if autoscaling_policies:
    html_autoscaling_policy += '''<div class="form-group">
            <label for="autoscaling_policy">Autoscaling Policy</label>
            <select name="autoscaling_policy" class="form-control">
            <option value="">None</option>'''
    for i in autoscaling_policies:
      html_autoscaling_policy += f'<option value="{i}">{i}</option>'
    html_autoscaling_policy += '''</select></div> '''

  html_pip_packages = """<div class="form-group">
        <label for="pip_packages">Install the following pip packages</label>
        <input name="pip_packages" class="form-control" placeholder="Example: 'pandas==0.23.0 scipy==1.1.0'"
              value=""></input>
    </div>"""

  html_condo_packages = """<div class="form-group">
      <label for="condo_packages">Install the following condo packages</label>
      <input name="condo_packages" class="form-control" placeholder="Example: 'scipy=1.1.0 tensorflow'"
            value=""></input>
  </div>"""

  html_internal_ip_only = """<div class="form-group">
    <label for="internal_ip_only">Internal IP only</label>
    <input name="internal_ip_only" type="checkbox"></input>
  </div>"""

  html_master_type = ""
  if node_types:
    html_master_type += """<div class="form-group">
        <label for="master_node_type">Master machine type</label>
        <select class="form-control" name="master_node_type">"""
    html_master_type += '''<option value="">Default</option>'''
    for t in node_types:
      html_master_type += f'''<option value="{t}">{t}</option>'''
    html_master_type += "</select></div>"

  html_master_disk_type = ""
  html_master_disk_type += """<div class="form-group">
      <label for="master_disk_type">Master disk type</label>
      <select class="form-control" name="master_disk_type">"""
  html_master_disk_type += '''<option value="">pd-standard</option>
      <option value="">pd-ssd</option>'''
  html_master_disk_type += "</select></div>"

  html_master_disc = """<div class="form-group">
      <label for="master_node_disc_size">Master disk size</label>
      <input name="master_node_disc_size" class="form-control" placeholder="default"
            value=""></input>
  </div>"""

  html_worker_type = ""
  if node_types:
    html_worker_type += """<div class="form-group">
        <label for="worker_node_type">Workers machine type</label>
        <select class="form-control" name="worker_node_type">"""
    html_worker_type += '''<option value="">Default</option>'''
    for t in node_types:
      html_worker_type += f'''<option value="{t}">{t}</option>'''
    html_worker_type += "</select></div>"

  html_worker_disk_type = ""
  html_worker_disk_type += """<div class="form-group">
      <label for="worker_disk_type">Worker disk type</label>
      <select class="form-control" name="worker_disk_type">"""
  html_worker_disk_type += '''<option value="">pd-standard</option>
      <option value="">pd-ssd</option>'''
  html_worker_disk_type += "</select></div>"

  html_worker_disc = """<div class="form-group">
      <label for="worker_node_disc_size">Worker disc size</label>
      <input name="worker_node_disc_size" class="form-control" placeholder="default"
            value=""></input>
  </div>"""

  html_worker_amount = """<div class="form-group">
      <label for="worker_node_amount">Amount of workers</label>
      <input name="worker_node_amount" class="form-control" placeholder="default"
            value=""></input>
  </div>"""

  html_custom_labels = """<div class="form-group">
      <label for="custom_labels">User defined labels</label>
      <input name="custom_labels" class="form-control" placeholder="key1:value1,key2:value2"
            value=""></input>
  </div>"""

  html_hive_settings = """<div class="form-group">
      <label for="hive_host">Hive Metastore host</label>
      <input name="hive_host" class="form-control" placeholder="" value=""></input>
  </div><div class="form-group">
      <label for="hive_db">Hive Metastore database</label>
      <input name="hive_db" class="form-control" placeholder="" value=""></input>
  </div><div class="form-group">
      <label for="hive_user">Hive Metastore user name</label>
      <input name="hive_user" class="form-control" placeholder="" value=""></input>
  </div><div class="form-group">
      <label for="hive_passwd">Hive Metastore password</label>
      <input name="hive_passwd" class="form-control" placeholder="" value=""></input>
  </div>"""

  body = "\n".join([
      html_autoscaling_policy,
      html_pip_packages,
      html_condo_packages,
      html_internal_ip_only,
      html_master_type,
      html_master_disk_type,
      html_master_disc,
      html_worker_type,
      html_worker_disk_type,
      html_worker_disc,
      html_worker_amount,
      html_custom_labels,
      html_hive_settings
  ])

  return head_html + js_code + body + bottom_html
