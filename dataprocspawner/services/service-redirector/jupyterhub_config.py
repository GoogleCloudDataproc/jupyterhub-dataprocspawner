# THIS FILE MUST BE APPENDED TO jupyterhub_config.py
import sys

c.JupyterHub.services = [
  {
    'name': 'redirector',
    'url': 'http://127.0.0.1:8888',
    'command': [sys.executable, "-m", "services/service-redirector/redirector"],
  }
]