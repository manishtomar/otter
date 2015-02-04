from __future__ import print_function
from twisted.trial.unittest import TestCase
from otter.auth import authenticate_user, public_endpoint_url, extract_token
from twisted.internet.task import deferLater
from twisted.internet import reactor
from twisted.internet.tcp import Client

from otter.util.http import (headers, append_segments, check_success)
import treq
import json
from twisted.web.client import HTTPConnectionPool
from characteristic import attributes


@attributes(['tenant_id', 'token', 'catalog'])
class UserInfo(object):
    """
    Capture data from the serviceCatalog in an object
    """
    @property
    def get_autoscale_endpoint(self):
        try:
            as_url = public_endpoint_url(self.catalog, 'autoscale', 'ORD')
        except StopIteration:
            as_url = 'http://localhost:9000/v1.0/{0}'.format(self.tenant_id)
        return as_url

    @property
    def get_nova_endpoint(self):
        """
        Return the nova endpoint - ASSUME MIMIC
        """
        try:
            nova_url = public_endpoint_url(self.catalog,
                                           'cloudServersOpenStack', 'ORD')
        except StopIteration:
            raise Exception('No Nova URL')
        return nova_url

    @classmethod
    def from_identity_response(cls, response):
        """
        Extract the token, public_endpoint_url and ...
        """
        my_token = extract_token(response)
        tenant_id = response['access']['token']['tenant']['id']
        return cls(tenant_id=tenant_id, token=my_token,
                   catalog=response['access']['serviceCatalog'])


# Helper functions

def _extract_from_identity_response(response):
    """
    Extract the token, public_endpoint_url and ...
    """
    my_token = extract_token(response)
    tenant_id = response['access']['token']['tenant']['id']
    return UserInfo(tenant_id=tenant_id, token=my_token,
                    catalog=response['access']['serviceCatalog'])


def create_scaling_group(user_info, group_config_blob, pool):
    """
    Using the given user info, create a scaling group with the
    config blob
    """
    d1 = treq.post(
        append_segments(user_info.get_autoscale_endpoint, 'groups'),
        json.dumps(group_config_blob),
        headers=headers(user_info.token),
        pool=pool)
    d1.addCallback(check_success, [201])
    d1.addCallback(treq.json_content)
    return d1

# Check with Nova to get a list of servers ids on the scaling group


def get_servers_using_nova_metadata(user_info, group_id):
    pass



class ConvergenceTestCase(TestCase):
    """
    """

    def setUp(self):
        self.pool = HTTPConnectionPool(reactor, False)

    def tearDown(self):
        def _check_fds(_):

            # Why is this getting readers twice?
            fds = set(reactor.getReaders() + reactor.getWriters())
            if not [fd for fd in fds if isinstance(fd, Client)]:
                return

            return deferLater(reactor, 0, _check_fds, None)

        return self.pool.closeCachedConnections()  # .addBoth(_check_fds)

    def test_fix_deleted_server(self):
        """
        """
        # Auth
        auth_endpoint = "http://localhost:8900/identity/v2.0"
        username = 'lk_trial'
        password = 'trialpass'

        d = authenticate_user(auth_endpoint, username, password,
                              pool=self.pool)



        d.addCallback(UserInfo.from_identity_response)

        # Create a scaling group
        group_blob = {
            "groupConfiguration": {
                "name": "workers",
                "cooldown": 0,
                "minEntities": 1
            },
            "launchConfiguration": {
                "type": "launch_server",
                "args": {
                    "server": {
                        "flavorRef": 'performance1-1',
                        "name": "A",
                        "imageRef": "aab63bcf-89aa-440f-b0c7-c7a1c611914b"
                    }
                }
            }
        }

        # If this works, d should now contain the json_content dict from the
        # group creation
        d.addCallback(create_scaling_group, group_blob, self.pool)




        d.addCallback(print)
        return d
