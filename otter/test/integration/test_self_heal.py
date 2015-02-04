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
from twisted.internet.defer import inlineCallbacks, gatherResults


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


def get_servers_using_nova_metadata(user_info, group_id, pool):
    nova_url = append_segments(user_info.get_nova_endpoint,
                               'servers',
                               'detail')
    print(nova_url)
    d2 = treq.get(nova_url, headers=headers(user_info.token), pool=pool)

    d2.addCallback(check_success, [200])
    d2.addCallback(treq.json_content)
    d2.addCallback(
        lambda details:
        [serv for serv in details['servers'] if
         serv['metadata'].get('rax:auto_scaling_group_id', None) == group_id])
    return d2


def delete_servers_using_nova(user_info, server_id_list, pool):
    nova_url_list = [append_segments(user_info.get_nova_endpoint,
                     'servers', sid) for sid in server_id_list]
    print(nova_url_list)
    deferreds = [
        treq.delete(nova_url, headers=headers(user_info.token), pool=pool)
            .addCallback(check_success, [204])
            .addCallback(treq.content)
        for nova_url in nova_url_list]

    d3 = gatherResults(deferreds)
    return d3




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

    @inlineCallbacks
    def test_fix_deleted_server(self):
        """
        """
        # Auth
        auth_endpoint = "http://localhost:8900/identity/v2.0"
        username = 'lk_trial'
        password = 'trialpass'

        identity_response = yield authenticate_user(auth_endpoint,
                                                    username, password,
                                                    pool=self.pool)



        user_data = UserInfo.from_identity_response(identity_response)

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
        group_response = yield create_scaling_group(user_data, group_blob, self.pool)
        print(group_response)

        # Check with Nova to confirm
        server_details = yield get_servers_using_nova_metadata(user_data,
            group_response['group']['id'], self.pool)

        self.assertEquals(len(server_details), 1)

        # Delete the server using Nova
        server_id_list = [s['id'] for s in server_details]

        print(server_id_list)
        delete_responses = yield delete_servers_using_nova(user_data,
                                                           server_id_list,
                                                           self.pool)
        print(delete_responses)




