"""Tests for otter.auth_http."""

import json
import sys

from effect.testing import resolve_effect
from effect import Delay
from effect.twisted import perform

from twisted.trial.unittest import SynchronousTestCase
from twisted.internet.defer import succeed

from otter.util.http import headers, APIError
from otter.auth_http import get_request_func, should_retry, bind_service
from otter.test.utils import stub_pure_response
from otter.util.pure_http import Request
from otter.test.worker.test_launch_server_v1 import fake_service_catalog


class FakeCachingAuthenticator(object):
    """
    Fake object that exposes caching side-effects.
    """
    def __init__(self):
        self.cache = {}

    def authenticate_tenant(self, tenant_id, log=None):
        """Put an entry in self.cache for the tenant."""
        result = 'token', fake_service_catalog
        self.cache[tenant_id] = result
        return succeed(result)

    def invalidate(self, tenant_id):
        """Delete an entry in self.cache"""
        del self.cache[tenant_id]


class GetRequestFuncTests(SynchronousTestCase):
    """
    Tests for :func:`get_request_func`.
    """

    def setUp(self):
        """Save some common parameters."""
        self.log = object()
        self.authenticator = FakeCachingAuthenticator()
        self.request = get_request_func(self.authenticator, 1, self.log)

    def test_get_request_func_authenticates(self):
        """
        The request function returned from get_request_func performs
        authentication before making the request.
        """
        eff = self.request('get', 'http://example.com/')
        # First there's a FuncIntent for the authentication
        next_eff = resolve_effect(eff, self.successResultOf(eff.intent.func()))
        # which causes the token to be cached
        self.assertEqual(self.authenticator.cache[1],
                         ('token', fake_service_catalog))
        # The next effect in the chain is the requested HTTP request,
        # with appropriate auth headers
        self.assertEqual(
            next_eff.intent,
            Request(method='get', url='http://example.com/',
                    headers=headers('token'), log=self.log))

    def test_invalidate_on_auth_error_code(self):
        """
        Upon authentication error, the auth cache is invalidated.
        """
        eff = self.request('get', 'http://example.com/')
        # First there's a FuncIntent for the authentication
        next_eff = resolve_effect(eff, self.successResultOf(eff.intent.func()))
        # which causes the token to be cached
        self.assertEqual(self.authenticator.cache[1],
                         ('token', fake_service_catalog))
        # When the HTTP response is an auth error, the auth cache is
        # invalidated, by way of the next effect:
        invalidate_effect = resolve_effect(next_eff, stub_pure_response("", 401))
        self.assertRaises(APIError, resolve_effect, invalidate_effect, invalidate_effect.intent.func())
        self.assertNotIn(1, self.authenticator.cache)

    def test_json(self):
        """
        Requests and responses are dumped and loaded.
        """
        input_json = {"a": 1}
        output_json = {"b": 2}
        eff = self.request("get", "http://google.com/", data=input_json)
        next_eff = resolve_effect(eff, self.successResultOf(eff.intent.func()))
        result = resolve_effect(next_eff,
                                stub_pure_response(json.dumps(output_json)))
        self.assertEqual(next_eff.intent.data, json.dumps(input_json))
        self.assertEqual(result, output_json)


class ShouldRetryTests(SynchronousTestCase):
    """Tests for :func:`should_retry`."""

    def _get_exc(self):
        """Get an exception tuple."""
        try:
            1 / 0
        except:
            return sys.exc_info()

    def test_should_retry_indeed(self):
        """
        :func:`should_retry` returns a function that returns an Effect of
        True when retrying is allowed, after delaying based on the
        ``next_interval`` argument.
        """
        eff = should_retry(lambda f: True, lambda: 1, self._get_exc())
        self.assertEqual(eff.intent, Delay(1))
        result = resolve_effect(eff, None)
        self.assertTrue(result)

    def test_failure_to_user_function(self):
        """
        The ``can_retry`` argument gets passed a failure correctly constructed
        out of the exception passed.
        """
        e = self._get_exc()
        failures = []

        def can_retry(f):
            failures.append(f)
            return True
        should_retry(can_retry, lambda: 1, e)
        [f] = failures
        self.assertEqual((f.type, f.value, f.tb), e)


class BindServiceTests(SynchronousTestCase):
    """Tests for :func:`bind_service`."""

    def setUp(self):
        """Save some common parameters."""
        self.log = object()
        self.authenticator = FakeCachingAuthenticator()
        self.request = lambda method, url, headers=None, data=None: (method, url, headers, data)

    def test_bind_service(self):
        """
        URL paths passed to the request function are appended to the
        endpoint of the service in the specified region for the tenant.
        """
        request = bind_service(self.request, '123', self.authenticator,
                               'cloudServersOpenStack', 'DFW', self.log)
        # - it should be bound to the endpoint URL
        # - that's all?
        self.assertEqual(
            self.successResultOf(perform(request('get', 'foo'))),
            ('get', 'http://dfw.openstack/foo', None, None))
