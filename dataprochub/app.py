#!/usr/bin/env python3
from jupyterhub.app import JupyterHub
from jupyterhub.handlers.base import UserUrlHandler, RequestHandler, BaseHandler
from tornado import gen, web, httputil
from jupyterhub import handlers, apihandlers
from jupyterhub.handlers.static import CacheControlStaticFilesHandler, LogoHandler
from jupyterhub.utils import (
    maybe_future,
    url_path_join,
    print_stacks,
    print_ps_info,
    make_ssl_context,
)
from urllib.parse import unquote
from urllib.parse import urlparse
from urllib.parse import urlunparse


class DataprocHubUserUrlHandler(UserUrlHandler):
  
  async def _redirect_to_user_server(self, user, spawner):
    self.statsd.incr('redirects.user_after_login')
    redirect_url = user.spawners[spawner.name].component_gateway_url
    self.log.info(f'# Redirecting to notebook at {redirect_url}.')
    self.redirect(redirect_url)


class DataprocHub(JupyterHub):
  
  HANLDERS_FILTER = (
    r'/user/(?P<user_name>[^/]+)(?P<user_path>/.*)?',
    r'/hub/user/(?P<user_name>[^/]+)(?P<user_path>/.*)?',
  )

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
  
  def init_handlers(self):
    """ Modifies the default app handlers. 
    
    Modifies the default behavior of JupyterHub user URL handler to use a custom 
    handler and allow Dataproc Hub to do redirect instead of proxying.

    Handlers are tuples of the form (regex_path, handler_class)
    """
    h = []
    h.extend(self.authenticator.get_handlers(self))
    h.extend(handlers.default_handlers)
    h.extend(apihandlers.default_handlers)
    h.extend(self.extra_handlers)
    h.append((r'/logo', LogoHandler, {'path': self.logo_file}))
    h.append((r'/api/(.*)', apihandlers.base.API404))

    # Replaces relevant handlers. This updates the original method.
    h = [c for c in h if c[0] not in self.HANLDERS_FILTER]
    h.append((self.HANLDERS_FILTER[0], DataprocHubUserUrlHandler))

    self.handlers = self.add_url_prefix(self.hub_prefix, h)
    self.handlers.extend(
      [
        # (r"%s(user|services)/([^/]+)" % self.base_url, handlers.AddSlashHandler),
        (r"(?!%s).*" % self.hub_prefix, handlers.PrefixRedirectHandler),
        (r'(.*)', handlers.Template404),
      ]
    )
    self.log.info(self.handlers)

main = DataprocHub.launch_instance

if __name__ == "__main__":
  main()