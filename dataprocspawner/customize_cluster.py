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

def get_html_top():
	html_base_top = '''
	<div class="jupyter-content mdc-typography">
		<div class="jupyter-form">
			<link
				rel="stylesheet"
				href="https://fonts.googleapis.com/icon?family=Material+Icons"
			/>
			<link rel="stylesheet" href="https://storage.googleapis.com/artifacts.pso-wmt-dp-spawner.appspot.com/index.css?v=4" />
			<br />
			<br />'''

	return html_base_top

def get_html_bottom():
	html_base_bottom = '''
		</div>
		</div>
		<script src="https://storage.googleapis.com/artifacts.pso-wmt-dp-spawner.appspot.com/index.js?v=4"></script>'''

	return html_base_bottom

def get_base_cluster_html_form(configs, locations_list, jupyterhub_region):
	"""
	Args:
		- List configs: List of Cloud Dataproc cluster config files path in GCS.
		- List locations_list: List of zones letters to choose from to create a
			Cloud Dataproc cluster in the JupyterHub region. ex: ["a", "b"]
	"""
	locations_list = [i for i in locations_list if i]
	configs = [i for i in configs if i]

	# Cluster configuration
	html_config = f'''
		<div class="mdc-select jupyter-select mdc-select--outlined">
			<input
				type="hidden"
				name="cluster_type"
				value="{configs[0]}"
			/>
			<div class="mdc-select__anchor">
				<span class="mdc-select__selected-text"></span>
				<span class="mdc-select__dropdown-icon">
					<svg
						width="10px"
						height="5px"
						viewBox="7 10 10 5"
						focusable="false"
					>
						<polygon
							class="mdc-select__dropdown-icon-inactive"
							stroke="none"
							fill-rule="evenodd"
							points="7 10 12 15 17 10"
						></polygon>
						<polygon
							class="mdc-select__dropdown-icon-active"
							stroke="none"
							fill-rule="evenodd"
							points="7 15 12 10 17 15"
						></polygon>
					</svg>
				</span>
				<span class="mdc-notched-outline">
					<span class="mdc-notched-outline__leading"></span>
					<span class="mdc-notched-outline__notch">
						<span class="mdc-floating-label"
							>Cluster's configuration</span
						>
					</span>
					<span class="mdc-notched-outline__trailing"></span>
				</span>
			</div>
		<div class="mdc-select__menu mdc-menu mdc-menu-surface">
			<ul class="mdc-list">'''

	for config in configs:
		name = ".".join(config.split("/")[-1].split(".")[:-1])
		html_config += f'''
			<li class="mdc-list-item" data-value="{config}">
				<span class="mdc-list-item__text">{name}</span>
			</li>'''

	html_config += '''
		</ul>
		</div>
		</div>
		<br />
		<br />'''

	# Zone configuration
	html_zone = f'''
		<div class="mdc-select jupyter-select mdc-select--outlined">
			<input
				type="hidden"
				name="cluster_zone"
				value="{jupyterhub_region}-{locations_list[0]}"
			/>
			<div class="mdc-select__anchor">
				<span class="mdc-select__selected-text"></span>
				<span class="mdc-select__dropdown-icon">
					<svg
						width="10px"
						height="5px"
						viewBox="7 10 10 5"
						focusable="false"
					>
						<polygon
							class="mdc-select__dropdown-icon-inactive"
							stroke="none"
							fill-rule="evenodd"
							points="7 10 12 15 17 10"
						></polygon>
						<polygon
							class="mdc-select__dropdown-icon-active"
							stroke="none"
							fill-rule="evenodd"
							points="7 15 12 10 17 15"
						></polygon>
					</svg>
				</span>
				<span class="mdc-notched-outline">
					<span class="mdc-notched-outline__leading"></span>
					<span class="mdc-notched-outline__notch">
						<span class="mdc-floating-label"
							>Zone</span
						>
					</span>
					<span class="mdc-notched-outline__trailing"></span>
				</span>
			</div>
		<div class="mdc-select__menu mdc-menu mdc-menu-surface">
			<ul class="mdc-list">'''

	for zone_letter in locations_list:
		location = f"{jupyterhub_region}-{zone_letter}"
		html_zone += f'''
			<li class="mdc-list-item" data-value="{location}">
				<span class="mdc-list-item__text">{location}</span>
			</li>'''

	html_zone += '''
		</ul>
		</div>
		</div>
		<br />
		<br />'''

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

	html_toggler_close = '''
		</div>
		</div>'''

	html_toggler = '''
		<div class="jupyter-cluster-customization-zone">
			<div class="jupyter-cluster-customization-zone__top">
				<button
					id="jupyter-cluster-customization-zone-toggler"
					class="mdc-button"
					aria-pressed="false"
				>
					<div class="mdc-button__ripple"></div>
					<i class="material-icons mdc-button__icon" aria-hidden="true"
						>add</i
					>
					<span class="mdc-button__label">Customize your cluster</span>
				</button>
			</div>
			<br />
			<br />
			<div
				id="jupyter-cluster-customization-zone-wrapper"
				class="jupyter-cluster-customization-zone__content"
				hidden
			>
				<input
					id="custom-cluster"
					name="custom_cluster"
					value="" type="hidden"
				/>'''

	# Autoscaling policies configuration
	html_autoscaling_policy = ''
	if autoscaling_policies:
		html_autoscaling_policy = f'''
			<div class="mdc-select jupyter-select mdc-select--outlined">
				<input
					type="hidden"
					name="autoscaling_policy"
					value="{autoscaling_policies[0]}"
				/>
				<div class="mdc-select__anchor">
					<span class="mdc-select__selected-text"></span>
					<span class="mdc-select__dropdown-icon">
						<svg
							width="10px"
							height="5px"
							viewBox="7 10 10 5"
							focusable="false"
						>
							<polygon
								class="mdc-select__dropdown-icon-inactive"
								stroke="none"
								fill-rule="evenodd"
								points="7 10 12 15 17 10"
							></polygon>
							<polygon
								class="mdc-select__dropdown-icon-active"
								stroke="none"
								fill-rule="evenodd"
								points="7 15 12 10 17 15"
							></polygon>
						</svg>
					</span>
					<span class="mdc-notched-outline">
						<span class="mdc-notched-outline__leading"></span>
						<span class="mdc-notched-outline__notch">
							<span class="mdc-floating-label"
								>Zone</span
							>
						</span>
						<span class="mdc-notched-outline__trailing"></span>
					</span>
				</div>
			<div class="mdc-select__menu mdc-menu mdc-menu-surface">
				<ul class="mdc-list">'''

		for policy in autoscaling_policies:
			html_autoscaling_policy += f'''
				<li class="mdc-list-item" data-value="{policy}">
					<span class="mdc-list-item__text">{policy}</span>
				</li>'''

		html_autoscaling_policy += '''
			</ul>
			</div>
			</div>
			<br />
			<br />'''

	html_pip_packages = '''
		<label
		class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
	>
		<span class="mdc-notched-outline">
			<span class="mdc-notched-outline__leading"></span>
			<span class="mdc-notched-outline__notch">
				<span class="mdc-floating-label" id="pip_packages"
					>Install the following pip packages</span
				>
			</span>
			<span class="mdc-notched-outline__trailing"></span>
		</span>
		<input
			type="text"
			class="mdc-text-field__input"
			name="pip_packages"
			aria-labelledby="pip_packages"
		/>
	</label>
	<div class="mdc-text-field-helper-line">
		<div class="mdc-text-field-helper-text" aria-hidden="true">
			Example: 'pandas==0.23.0 scipy==1.1.0'
		</div>
	</div>
	<br />'''

	html_condo_packages = '''
			<label
				class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
			>
				<span class="mdc-notched-outline">
					<span class="mdc-notched-outline__leading"></span>
					<span class="mdc-notched-outline__notch">
						<span class="mdc-floating-label" id="condo_packages"
							>Install the following condo packages</span
						>
					</span>
					<span class="mdc-notched-outline__trailing"></span>
				</span>
				<input
					type="text"
					class="mdc-text-field__input"
					name="condo_packages"
					aria-labelledby="condo_packages"
				/>
			</label>
			<div class="mdc-text-field-helper-line">
				<div class="mdc-text-field-helper-text" aria-hidden="true">
					Example: 'scipy=1.1.0 tensorflow'
				</div>
			</div>
			<br />
			<br />
			<hr class="jupyter-hr" />
			<br />
			<br />'''

	html_master_base = '''
		<h2 class="jupyter-form__group-title">Master node</h2>
		<br />'''

	html_master_type = f'''
		<div class="mdc-select jupyter-select mdc-select--outlined">
			<input
				type="hidden"
				name="master_node_type"
				value="{node_types[0]}"
			/>
			<div class="mdc-select__anchor">
				<span class="mdc-select__selected-text"></span>
				<span class="mdc-select__dropdown-icon">
					<svg
						width="10px"
						height="5px"
						viewBox="7 10 10 5"
						focusable="false"
					>
						<polygon
							class="mdc-select__dropdown-icon-inactive"
							stroke="none"
							fill-rule="evenodd"
							points="7 10 12 15 17 10"
						></polygon>
						<polygon
							class="mdc-select__dropdown-icon-active"
							stroke="none"
							fill-rule="evenodd"
							points="7 15 12 10 17 15"
						></polygon>
					</svg>
				</span>
				<span class="mdc-notched-outline">
					<span class="mdc-notched-outline__leading"></span>
					<span class="mdc-notched-outline__notch">
						<span class="mdc-floating-label"
							>Machine type</span
						>
					</span>
					<span class="mdc-notched-outline__trailing"></span>
				</span>
			</div>
		<div class="mdc-select__menu mdc-menu mdc-menu-surface">
			<ul class="mdc-list">'''

	for type in node_types:
		html_master_type += f'''
			<li class="mdc-list-item" data-value="{type}">
				<span class="mdc-list-item__text">{type}</span>
			</li>'''

	html_master_type += '''
		</ul>
		</div>
		</div>
		<br />
		<br />'''

	html_master_disk_type = '''
		<div class="mdc-select jupyter-select mdc-select--outlined">
			<input
				type="hidden"
				name="master_disk_type"
				value="pd-standard"
			/>
			<div class="mdc-select__anchor">
				<span class="mdc-select__selected-text"></span>
				<span class="mdc-select__dropdown-icon">
					<svg
						width="10px"
						height="5px"
						viewBox="7 10 10 5"
						focusable="false"
					>
						<polygon
							class="mdc-select__dropdown-icon-inactive"
							stroke="none"
							fill-rule="evenodd"
							points="7 10 12 15 17 10"
						></polygon>
						<polygon
							class="mdc-select__dropdown-icon-active"
							stroke="none"
							fill-rule="evenodd"
							points="7 15 12 10 17 15"
						></polygon>
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
		<br />'''

	html_master_disk_size = '''
		<label
			class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
		>
			<span class="mdc-notched-outline">
				<span class="mdc-notched-outline__leading"></span>
				<span class="mdc-notched-outline__notch">
					<span class="mdc-floating-label" id="master_disk_size"
						>Master disk size</span
					>
				</span>
				<span class="mdc-notched-outline__trailing"></span>
			</span>
			<input
				type="text"
				class="mdc-text-field__input"
				name="master_disk_size"
				aria-labelledby="master_disk_size"
			/>
		</label>
		<br />
		<br />
		<hr class="jupyter-hr" />
		<br />
		<br />'''

	html_worker_base = '''
		<h2 class="jupyter-form__group-title">Worker nodes</h2>
		<br />'''

	html_worker_type = f'''
		<div class="mdc-select jupyter-select mdc-select--outlined">
			<input
				type="hidden"
				name="worker_node_type"
				value="{node_types[0]}"
			/>
			<div class="mdc-select__anchor">
				<span class="mdc-select__selected-text"></span>
				<span class="mdc-select__dropdown-icon">
					<svg
						width="10px"
						height="5px"
						viewBox="7 10 10 5"
						focusable="false"
					>
						<polygon
							class="mdc-select__dropdown-icon-inactive"
							stroke="none"
							fill-rule="evenodd"
							points="7 10 12 15 17 10"
						></polygon>
						<polygon
							class="mdc-select__dropdown-icon-active"
							stroke="none"
							fill-rule="evenodd"
							points="7 15 12 10 17 15"
						></polygon>
					</svg>
				</span>
				<span class="mdc-notched-outline">
					<span class="mdc-notched-outline__leading"></span>
					<span class="mdc-notched-outline__notch">
						<span class="mdc-floating-label"
							>Machine type</span
						>
					</span>
					<span class="mdc-notched-outline__trailing"></span>
				</span>
			</div>
		<div class="mdc-select__menu mdc-menu mdc-menu-surface">
			<ul class="mdc-list">'''

	for type in node_types:
		html_worker_type += f'''
			<li class="mdc-list-item" data-value="{type}">
				<span class="mdc-list-item__text">{type}</span>
			</li>'''

	html_worker_type += '''
		</ul>
		</div>
		</div>
		<br />
		<br />'''

	html_worker_disk_type = '''
		<div class="mdc-select jupyter-select mdc-select--outlined">
			<input
				type="hidden"
				name="worker_disk_type"
				value="pd-standard"
			/>
			<div class="mdc-select__anchor">
				<span class="mdc-select__selected-text">pd-standard</span>
				<span class="mdc-select__dropdown-icon">
					<svg
						width="10px"
						height="5px"
						viewBox="7 10 10 5"
						focusable="false"
					>
						<polygon
							class="mdc-select__dropdown-icon-inactive"
							stroke="none"
							fill-rule="evenodd"
							points="7 10 12 15 17 10"
						></polygon>
						<polygon
							class="mdc-select__dropdown-icon-active"
							stroke="none"
							fill-rule="evenodd"
							points="7 15 12 10 17 15"
						></polygon>
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
		<br />'''

	html_worker_disk_size = '''
		<label
			class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
		>
			<span class="mdc-notched-outline">
				<span class="mdc-notched-outline__leading"></span>
				<span class="mdc-notched-outline__notch">
					<span class="mdc-floating-label" id="worker_disk_size"
						>Master disk size</span
					>
				</span>
				<span class="mdc-notched-outline__trailing"></span>
			</span>
			<input
				type="text"
				class="mdc-text-field__input"
				name="worker_disk_size"
				aria-labelledby="worker_disk_size"
			/>
		</label>
		<br />
		<br />'''

	html_worker_amount = '''
		<label
			class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
		>
			<span class="mdc-notched-outline">
				<span class="mdc-notched-outline__leading"></span>
				<span class="mdc-notched-outline__notch">
					<span class="mdc-floating-label" id="worker_node_amount"
						>Number of worker nodes</span
					>
				</span>
				<span class="mdc-notched-outline__trailing"></span>
			</span>
			<input
				type="text"
				class="mdc-text-field__input"
				name="worker_node_amount"
				aria-labelledby="worker_node_amount"
			/>
		</label>
		<br />
		<br />
		<hr class="jupyter-hr" />
		<br />
		<br />'''

	html_adv_base = '''
		<h2 class="jupyter-form__group-title">Advanced</h2>
		<br />'''

	html_internal_ip_only = '''
		<div class="mdc-form-field">
			<div class="mdc-checkbox">
				<input
					type="checkbox"
					class="mdc-checkbox__native-control"
					id="internal_ip_only"
					name="internal_ip_only"
				/>
				<div class="mdc-checkbox__background">
					<svg class="mdc-checkbox__checkmark" viewBox="0 0 24 24">
						<path
							class="mdc-checkbox__checkmark-path"
							fill="none"
							d="M1.73,12.91 8.1,19.28 22.79,4.59"
						/>
					</svg>
					<div class="mdc-checkbox__mixedmark"></div>
				</div>
				<div class="mdc-checkbox__ripple"></div>
			</div>
			<label for="internal_ip_only">Configure all instances to have only internal IP addresses.</label>
		</div>
		<br />
		<br />'''

	html_custom_labels = '''
		<label
			class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
		>
			<span class="mdc-notched-outline">
				<span class="mdc-notched-outline__leading"></span>
				<span class="mdc-notched-outline__notch">
					<span class="mdc-floating-label" id="custom_labels"
						>User defined labels</span
					>
				</span>
				<span class="mdc-notched-outline__trailing"></span>
			</span>
			<input
				type="text"
				class="mdc-text-field__input"
				name="custom_labels"
				aria-labelledby="custom_labels"
			/>
		</label>
		<div class="mdc-text-field-helper-line">
			<div class="mdc-text-field-helper-text" aria-hidden="true">
				Example: 'key1:value1,key2:value2'
			</div>
		</div>
		<br />'''

	html_hive_settings = '''
		<label
			class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
		>
			<span class="mdc-notched-outline">
				<span class="mdc-notched-outline__leading"></span>
				<span class="mdc-notched-outline__notch">
					<span class="mdc-floating-label" id="hive_host"
						>Hive Metastore host</span
					>
				</span>
				<span class="mdc-notched-outline__trailing"></span>
			</span>
			<input
				type="text"
				class="mdc-text-field__input"
				name="hive_host"
				aria-labelledby="hive_host"
			/>
		</label>
		<br />
		<br />
		<label
			class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
		>
			<span class="mdc-notched-outline">
				<span class="mdc-notched-outline__leading"></span>
				<span class="mdc-notched-outline__notch">
					<span class="mdc-floating-label" id="hive_db"
						>Hive Metastore database</span
					>
				</span>
				<span class="mdc-notched-outline__trailing"></span>
			</span>
			<input
				type="text"
				class="mdc-text-field__input"
				name="hive_db"
				aria-labelledby="hive_db"
			/>
		</label>
		<br />
		<br />
		<label
			class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
		>
			<span class="mdc-notched-outline">
				<span class="mdc-notched-outline__leading"></span>
				<span class="mdc-notched-outline__notch">
					<span class="mdc-floating-label" id="hive_user"
						>Hive Metastore user name</span
					>
				</span>
				<span class="mdc-notched-outline__trailing"></span>
			</span>
			<input
				type="text"
				class="mdc-text-field__input"
				name="hive_user"
				aria-labelledby="hive_user"
			/>
		</label>
		<br />
		<br />
		<label
			class="mdc-text-field jupyter-text-field mdc-text-field--outlined"
		>
			<span class="mdc-notched-outline">
				<span class="mdc-notched-outline__leading"></span>
				<span class="mdc-notched-outline__notch">
					<span class="mdc-floating-label" id="hive_passwd"
						>Hive Metastore password</span
					>
				</span>
				<span class="mdc-notched-outline__trailing"></span>
			</span>
			<input
				type="text"
				class="mdc-text-field__input"
				name="hive_passwd"
				aria-labelledby="hive_passwd"
			/>
		</label>
		<br />
		<br />'''

	body = "\n".join([
			html_autoscaling_policy,
			html_pip_packages,
			html_condo_packages,
			html_master_base,
			html_master_type,
			html_master_disk_type,
			html_master_disk_size,
			html_worker_base,
			html_worker_type,
			html_worker_disk_type,
			html_worker_disk_size,
			html_worker_amount,
			html_adv_base,
			html_internal_ip_only,
			html_custom_labels,
			html_hive_settings
	])

	return html_toggler + body + html_toggler_close
