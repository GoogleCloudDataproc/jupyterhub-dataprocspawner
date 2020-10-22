#!/usr/bin/env python3
import json
import os
from urllib.parse import urlparse

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import Application
from tornado.web import authenticated
from tornado.web import RequestHandler

from jupyterhub.services.auth import HubAuthenticated

from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.options import define, options, parse_command_line
from tornado.gen import coroutine, multi


class RedirectorHandler(HubAuthenticated, RequestHandler):
    # hub_users can be a set of users who are allowed to access the service
    # `getuser()` here would mean only the user who started the service
    # can access the service:

    # from getpass import getuser
    # hub_users = {getuser()}

    @authenticated
    def get(self):
        # TODO(try in cloud)
        self.get_users()

        user_model = self.get_current_user()
        self.set_header('content-type', 'application/json')
        self.write(json.dumps(user_model, indent=1, sort_keys=True))
    
    def get_users(self):
        # self.redirect('http://www.lemonde.fr')
        api_url = os.environ['JUPYTERHUB_API_URL']
        api_token = os.environ['JUPYTERHUB_API_TOKEN']
        import requests
        r = requests.get(api_url + '/user', headers={'Authorization': 'token %s' % api_token,})

        r.raise_for_status()
        users = r.json()
        print(users)

        # client = AsyncHTTPClient()
        # api_token = os.environ['JUPYTERHUB_API_TOKEN']
        # print(f'# api_url is {api_url}')
        # print(f'# api_token is {api_token}')
        # auth_header = {'Authorization': 'token %s' % api_token}
        # req = HTTPRequest(url=api_url + '/users', headers=auth_header)
        # resp = yield client.fetch(req)
        # users = json.loads(resp.body.decode('utf8', 'replace'))
        # print(f'# users are {users}')


def main():
    app = Application(
        [
            (os.environ.get('JUPYTERHUB_SERVICE_PREFIX', '') + '/?', RedirectorHandler),
            (r'.*', RedirectorHandler),
        ]
    )

    http_server = HTTPServer(app)
    url = urlparse(os.environ['JUPYTERHUB_SERVICE_URL'])

    http_server.listen(url.port, url.hostname)

    IOLoop.current().start()


if __name__ == '__main__':
    main()
