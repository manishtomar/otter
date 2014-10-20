"""
Integration point for HTTP clients in otter.
"""
import json
from functools import partial

from effect import Effect, FuncIntent

from toolz.functoolz import compose
from toolz.dicttoolz import merge

from otter.util.pure_http import (
    get_request, request_with_auth, check_status)
from otter.util.http import headers


def get_request_func(authenticator, tenant_id, log):
    """
    Return a pure_http.Request-returning function decorated with:

    - authentication for Rackspace APIs
    - HTTP status code checking
    - JSON bodies and return values
    - returning only content of the result, not response objects
    - TODO: retries
    """
    unsafe_auth = partial(authenticator.authenticate_tenant, tenant_id, log=log)
    unsafe_invalidate = partial(authenticator.invalidate, tenant_id)
    auth_headers = Effect(FuncIntent(unsafe_auth)).on(success=headers)
    invalidate = Effect(FuncIntent(unsafe_invalidate))
    default_log = log

    def request(method, url, headers=None, data=None, log=default_log,
                reauth_codes=(401, 403),
                success_codes=(200,)):
        """
        Make an HTTP request, with a bunch of awesome behavior!

        :param bytes method: as :func:`get_request`.
        :param url: as :func:`get_request`.
        :param dict headers: as :func:`get_request`, but will have
            authentication headers added.
        :param data: JSON-able object.
        :param log: as :func:`get_request`.
        :param sequence success_codes: HTTP codes to consider successful.
        :param sequence reauth_codes: HTTP codes upon which to invalidate the
            auth cache.

        :raise APIError: When the response HTTP code is not in success_codes.
        :return: JSON-parsed object.
        """
        data = json.dumps(data) if data is not None else None
        request_with_headers = lambda h: get_request(method, url, headers=h,
                                                     data=data, log=log)
        return request_with_auth(
            request_with_headers,
            auth_headers,
            invalidate,
            headers=headers,
            reauth_codes=reauth_codes,
        ).on(partial(check_status, success_codes)
        ).on(lambda result: result[1]
        ).on(json.loads)

    return request
