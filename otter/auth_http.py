"""
Integration point for HTTP clients in otter.
"""
from functools import partial

from effect import Effect, FuncIntent

from toolz.functoolz import compose

from otter.util.pure_http import (
    get_request, request_with_auth, request_with_status_check,
    request_with_json, content_request)
from otter.util.http import headers


def get_request_func(authenticator, tenant_id, log):
    """
    Return a pure_http.Request-returning function decorated with:

    - authentication for Rackspace APIs
    - HTTP status code checking
    - JSON bodies and return values
    - returning only content of the result, not response objects.

    Integration point!

    The request function will take the following arguments:

    - method (bytes)
    - url (bytes)
    - data (json-able) default None
    - headers (dict) default {}
    - success_codes (sequence) HTTP codes to consider successful; default is
        [200].
    - reauth_codes (sequence) HTTP codes upon which to invalidate the auth
        cache, default is [401, 403].
    """
    unsafe_auth = partial(authenticator.authenticate_tenant, tenant_id, log=log)
    unsafe_invalidate = partial(authenticator.invalidate, tenant_id)
    auth_headers = Effect(FuncIntent(unsafe_auth)).on(success=headers)
    invalidate = Effect(FuncIntent(unsafe_invalidate))
    request = partial(
        request_with_auth,
        get_request,
        get_auth_headers=lambda: auth_headers,
        refresh_auth_info=lambda: invalidate,
    )
    request = partial(request_with_status_check, request)
    request = partial(request_with_json, request)
    request = compose(content_request, request)
    return request
