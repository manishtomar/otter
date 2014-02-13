"""
Worker that abstracts some of functionality to update an otter stack and
converge.
"""

from otter.util.config import config_value
from otter.util.hashkey import generate_server_name
from otter.util.http import append_segments
from otter.worker.launch_server_v1 import prepare_launch_config
from otter.worker.heat_client import HeatClient
from otter.worker.heat_template import generate_template


class HeatWorker(object):
    def __init__(self, tenant_id, group_id, launch_config, desired,
                 auth_token, log=None):
        self.group_id = group_id
        self.tenant_id = tenant_id
        self.launch_config = launch_config
        self.launch_config['args'] = prepare_launch_config(
            self.group_id, launch_config['args'])
        self.desired = desired
        self.client = HeatClient(auth_token, log)

    def create_stack(self):
        """
        Creates a stack with a template generated from the launch config and
        desired capacity.
        """
        # stack names must contain only alphanumeric or _-. characters,
        # and must start with alpha\
        stack_name = 'Otter-{0}-{1}'.format(self.group_id,
                                            generate_server_name())
        template = generate_template(self.launch_config, self.desired)
        url = append_segments(config_value('heat.url'), self.tenant_id)
        d = self.client.create_stack(url, stack_name, environment={},
                                     files={}, parameters={}, timeout=60,
                                     disable_rollback=True, template=template)

        def get_link(response_body):
            links = [link for link in response_body['stack']['links']
                     if link['rel'] == 'self']
            return links[0]['href']

        return d.addCallback(get_link)
