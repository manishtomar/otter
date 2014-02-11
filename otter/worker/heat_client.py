"""
Asynchronous client for Heat, using treq.
"""

from __future__ import print_function


import json

from otter.util import logging_treq as treq
from otter.util.http import (append_segments, headers, check_success,
                             wrap_request_error, raise_error_on_code,
                             APIError, RequestError)


class MutuallyExclusiveArgumentsError(Exception):
    def __init__(self, args):
        self.args = args
        super(MutuallyExclusiveArgumentsError, self).__init___(
            "%s are mutually exclusive" % (self.args,))


class HeatClient(object):
    def __init__(self, auth_token, tenant_id, log):
        self.heat_url = heat_url
        self.tenant_id = tenant_id
        self.auth_token = auth_token
        self.log = log.bind(tenant_id=tenant_id, heat_url=heat_url, heatclient=True)

    def create_stack(self, heat_url, stack_name, environment, files,
                     parameters, timeout, disable_rollback,
                     template=None, template_url=None):
        """Create a stack!"""
        payload = {
            "stack_name": stack_name,
            "parameters": parameters,
            "timeout_mins": timeout
        }
        if template is not None:
            payload['template'] = template
        elif template_url is not None:
            payload['template_url'] = template_url
        else:
            raise MutuallyExclusiveArgumentsError('template', 'template_url')
        log = self.log.bind(event='create-stack', stack_name=stack_name)
        result = treq.post(
            append_segments(heat_url, 'stacks'),
            data=json.dumps(payload),
            headers=headers(self.auth_token), log=log)
        result = result.addCallback(check_success, [200, 201])
        return result.addCallback(treq.json_content)

    def update_stack(self, stack_name, environment, files, parameters, timeout,
                     template=None, template_url=None):
        """Update a stack!"""
        payload = {
            "stack_name": stack_name,
            "parameters": parameters,
            "timeout_mins": timeout
        }
        if template is not None:
            payload['template'] = template
        elif template_url is not None:
            payload['template_url'] = template_url
        else:
            raise MutuallyExclusiveArgumentsError('template', 'template_url')
        log = self.log.bind(event='update-stack', stack_name=stack_name)
        # v1/{tenant_id}/stacks/{stack_name}/{stack_id}
        treq.post(self._url('stacks', stack_name),
                      data=json.dumps(payload),
                      headers=headers(self.auth_token),
                      log=log)
        return d

    def get_stack_id(self, stack_name):
        """
        Get the stack ID from the stack name.

        Usually this isn't necessary.
        """
        def _got_stack_id_result(response):
            assert response.code == 302
            return response.headers['Location']

        result = treq.get(self._url('stacks', stack_name),
                          headers=headers(self.auth_token),
                          allow_redirects=False)
        return result.addCallback(_got_stack_id_result)

    def get_stack(self, stack_name):
        """Get the metadata about a stack."""


    def get_resources(self, stack_name):
        """
        Get a list of resources from a stack.
        """

    def delete_stack(self, stack_name):
        """Delete a stack."""


def main(reactor, *args):
    import os, yaml
    from otter.log import log
    template = yaml.safe_load(open(args[1]))
    client = HeatClient(
        os.environ['OS_AUTH_TOKEN'],
        'https://dfw.orchestration.api.rackspacecloud.com/',
        os.environ['OS_TENANT_ID'],
        log)

    result = client.create_stack(
        'my-stack-name', None, None, {}, 60, False, template=template)
    return result.addCallback(print)


if __name__ == '__main__':
    import sys
    from twisted.internet.task import react
    from twisted.python.log import startLogging, addObserver
    from otter.log.setup import observer_factory
    addObserver(observer_factory())
    react(main, sys.argv)
