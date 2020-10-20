import argparse
import datetime
import json
import os

from tornado import escape
from tornado import ioloop
from tornado import web

from jupyterhub.services.auth import HubAuthenticated

class RedirectorRequestHandler(HubAuthenticated, web.RequestHandler):

  @web.authenticated
  def get(self):
    self.redirect('http://www.lemonde.fr')

def main():
    args = parse_arguments()
    application = create_application(**vars(args))
    application.listen(args.port)
    ioloop.IOLoop.current().start()


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-prefix",
        "-a",
        default=os.environ.get("JUPYTERHUB_SERVICE_PREFIX", "/"),
        help="application API prefix",
    )
    parser.add_argument(
        "--port", "-p", default=8888, help="port for API to listen on", type=int
    )
    return parser.parse_args()


def create_application(api_prefix="/", handler=RedirectorRequestHandler, **kwargs):
    return web.Application([(api_prefix, handler)])


if __name__ == "__main__":
    main()
