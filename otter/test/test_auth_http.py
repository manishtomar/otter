"""Tests for otter.auth_http."""

from twisted.trial.unittest import SynchronousTestCase

from effect.testing import resolve_effect

from twisted.internet.defer import succeed

from otter.util.http import headers, APIError
from otter.auth_http import get_request_func
from otter.test.utils import stub_pure_response
from otter.util.pure_http import Request


class FakeCachingAuthenticator(object):
    """
    Fake object that exposes caching side-effects.
    """
    def __init__(self):
        self.cache = {}

    def authenticate_tenant(self, tenant_id, log=None):
        """Put an entry in self.cache for the tenant."""
        token = 'token'
        self.cache[tenant_id] = token
        return succeed(token)

    def invalidate(self, tenant_id):
        """Delete an entry in self.cache"""
        del self.cache[tenant_id]


class GetRequestFuncTests(SynchronousTestCase):
    """
    Tests for get_request_func
    """
    def test_get_request_func_authenticates(self):
        """
        The request function returned from get_request_func performs
        authentication before making the request.
        """
        log = object()
        authenticator = FakeCachingAuthenticator()
        request = get_request_func(authenticator, 1, log)
        eff = request('get', 'http://example.com/')
        # First there's a FuncIntent for the authentication
        next_eff = resolve_effect(eff, self.successResultOf(eff.intent.func()))
        # which causes the token to be cached
        self.assertEqual(authenticator.cache[1], 'token')
        # The next effect in the chain is the requested HTTP request,
        # with appropriate auth headers
        self.assertEqual(
            next_eff.intent,
            Request(method='get', url='http://example.com/',
                    headers=headers('token')))

    def test_invalidate_on_auth_error_code(self):
        """
        Upon authentication error, the auth cache is invalidated.
        """
        log = object()
        authenticator = FakeCachingAuthenticator()
        request = get_request_func(authenticator, 1, log)
        eff = request('get', 'http://example.com/')
        # First there's a FuncIntent for the authentication
        next_eff = resolve_effect(eff, self.successResultOf(eff.intent.func()))
        # which causes the token to be cached
        self.assertEqual(authenticator.cache[1], 'token')
        # When the HTTP response is an auth error, the auth cache is
        # invalidated, by way of the next effect:
        invalidate_effect = resolve_effect(next_eff, stub_pure_response("", 401))
        self.assertRaises(APIError, resolve_effect, invalidate_effect, invalidate_effect.intent.func())
        self.assertNotIn(1, authenticator.cache)
