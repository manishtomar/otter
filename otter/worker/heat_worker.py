"""
Worker that abstracts some of functionality to update an otter stack and
converge.
"""

from otter.util.config import config_value
from otter.worker.heat_client import HeatClient
from otter.worker.heat_template import generate_template
    

class HeatWorker(object):
    def __init__(self, tenant_id, config, launch_config, desired, auth_token, 
                 log=None):
        self.config = config
        self.launch_config = launch_config
        self.desired = desired
        self.client = HeatClient(auth_token, self.tenant_id, log)
        
    def create_stack(self):
        """
        Creates a stack with a template generated from the launch config and
        desired capacity.
        """
        stack_name = 'Otter:{0}'.format(self.config['name'])
        template = generate_template(self.launch_config, self.desired)
        d = self.client.create_stack(config_value('heat_url'), stack_name, 
                                     files={}, parameters={}, timeout=60, 
                                     disable_rollback=True, template=template)
        
        def get_link(response_body):
            links = [link for link in response_body['stack'][links]
                     if link['rel'] == 'self']
            return links[0]['href']
            
        return d.addCallback(get_link)
        
        