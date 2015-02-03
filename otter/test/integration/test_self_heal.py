from __future__ import print_function
from twisted.trial.unittest import TestCase
from otter.auth import authenticate_user, public_endpoint_url, extract_token
from twisted.internet.task import deferLater
from twisted.internet import reactor
from twisted.internet.tcp import Client

from otter.util.http import (headers, append_segments, check_success)
import treq

from twisted.web.client import HTTPConnectionPool, ResponseFailed


class ConvergenceTestCase(TestCase):
    """
    """

    def setUp(self):
        self.pool = HTTPConnectionPool(reactor, False)

    def tearDown(self):
        def _check_fds(_):

            # Why is this getting readers twice?
            fds = set(reactor.getReaders() + reactor.getReaders())
            if not [fd for fd in fds if isinstance(fd, Client)]:
                return

            return deferLater(reactor, 0, _check_fds, None)

        return self.pool.closeCachedConnections().addBoth(_check_fds)

    def test_fix_deleted_server(self):
        """
        """
        # Auth
        auth_endpoint = "http://localhost:8900/identity/v2.0"
        username = 'lk_trial'
        password = 'trialpass'

        msg = 'The test has been entered'
        print(msg)
        d = authenticate_user(auth_endpoint, username, password, pool=self.pool)

        def _extract_from_identity_response(response):
            """
            Extract the token, public_endpoint_url and ...
            """
            my_token = extract_token(response)
            tenant_id = response['access']['token']['tenant']['id']
            try:
               as_url = public_endpoint_url(response['access']['serviceCatalog'],
                                'autoscale', 'ORD')
            except StopIteration:
                as_url = 'http://localhost:9000/v1.0/{0}'.format(tenant_id)
            return (my_token, tenant_id, as_url)

        def create_scaling_group(user_info_tuple, group_config_blob):
            """
            Using the given user info, create a scaling group with the config blob
            """
            d1 = treq.post(
                 append_segments(user_info_tuple[2], 'groups'),
                 json.dumps(group_config_blob),
                 headers=headers(user_info_tuple[0]))





        d.addCallback(_extract_from_identity_response)
        d.addCallback(create_scaling_group)
        d.addCallback(check_success, [201])
        d.addCallback(print)


        return d




