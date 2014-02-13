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


class UpdateInProgressError(Exception):
    """
    Exception to be raised when a stack is already updating and cannot be
    updated.
    """
    def __init__(self, stack_url):
        super(UpdateInProgressError, self).__init__(
            '{0} is being updated and cannot be updated again until done.'
            .format(stack_url))


class HeatClient(object):
    def __init__(self, auth_token, log):
        self.auth_token = auth_token
        self.log = log.bind(heatclient=True)

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
        result.addCallback(check_success, [200, 201])
        return result.addCallback(treq.json_content)

    def update_stack(self, stack_url, environment, files,
                     parameters, timeout, template=None, template_url=None):
        """Update a stack!"""
        payload = {
            "parameters": parameters,
            "timeout_mins": timeout
        }
        if template is not None:
            payload['template'] = template
        elif template_url is not None:
            payload['template_url'] = template_url
        else:
            raise MutuallyExclusiveArgumentsError('template', 'template_url')
        log = self.log.bind(event='update-stack')
        result = treq.put(stack_url,
                  data=json.dumps(payload),
                  headers=headers(self.auth_token),
                  log=log)

        # 202 is success
        result.addCallback(check_success, [202])
        result.addCallback(treq.content)  # 202 is no content

        # HEAT BUG: if the stack is already being updated, returns
        # a 400 with a message rather than a 410 or something.
        # {
        #     "explanation": "The server could not comply with the request since it is either malformed or otherwise incorrect.",
        #     "code": 400,
        #     "error": {
        #         "message": "Updating a stack when another action is in progress is not supported.",
        #         "traceback": "Traceback (most recent call last):\n\n  File \"/opt/heat/lib/python2.7/site-packages/heat/openstack/common/rpc/amqp.py\", line 462, in _process_data\n    **args)\n\n  File \"/opt/heat/lib/python2.7/site-packages/heat/openstack/common/rpc/dispatcher.py\", line 172, in dispatch\n    result = getattr(proxyobj, method)(ctxt, **kwargs)\n\n  File \"/opt/heat/lib/python2.7/site-packages/heat/engine/service.py\", line 63, in wrapped\n    return func(self, ctx, *args, **kwargs)\n\n  File \"/opt/heat/lib/python2.7/site-packages/heat/engine/service.py\", line 436, in update_stack\n    raise exception.NotSupported(feature=msg)\n\nNotSupported: Updating a stack when another action is in progress is not supported.\n",
        #         "type": "NotSupported"
        #     },
        #     "title": "Bad Request"
        # }
        def check_update_in_progress(failure):
            failure.trap(APIError)
            if failure.value.code == 400:
                json_body = json.loads(failure.value.body)
                if ('error' in json_body and 'message' in json_body['error']
                    and json_body['error']['message'].startswith(
                        "Updating a stack when another action is in progress "
                        "is not supported")):
                    raise UpdateInProgressError(stack_url)
            return failure

        result.addErrback(check_update_in_progress)
        # If the update comes immediately on the heels of another update,
        # a 409 is returned instead (conflict).  A little while after is the
        # 400.
        result.addErrback(raise_error_on_code, 409,
                          UpdateInProgressError(stack_url), 'stack_update')
        return result

    def get_stack(self, stack_url):
        """Get the metadata about a stack."""
        result = treq.get(stack_url, headers=headers(self.auth_token),
                          log=self.log)
        result.addCallback(check_success, [200])
        result.addCallback(treq.json_content)
        return result

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
    tenant = os.environ['OS_TENANT_ID']
    client = HeatClient(os.environ['OS_AUTH_TOKEN'], log)

    heat_root = 'https://dfw.orchestration.api.rackspacecloud.com/v1/' + tenant
    stack_url = heat_root + '/stacks/my-stack-name'
    result = client.get_stack(stack_url)
    def got_stack(result):
        print("here's a stack:", result)
        result = client.update_stack(stack_url, None, None,
                                     {}, 60, template=template)
        return result
    def no_stack(failure):
        failure.trap(APIError)
        if failure.value.code != 404:
            return failure
        result = client.create_stack(
             heat_root,
            'my-stack-name', None, None, {}, 60, False, template=template)
        return result
    result.addCallback(got_stack).addErrback(no_stack)
    return result.addCallback(lambda r: print("FINAL RESULT", r))


if __name__ == '__main__':
    import sys
    from twisted.internet.task import react
    from twisted.python.log import startLogging, addObserver
    from otter.log.setup import observer_factory
    addObserver(observer_factory())
    react(main, sys.argv)
