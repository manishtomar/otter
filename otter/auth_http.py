"""
Integration point for HTTP clients in otter.
"""
import json
from functools import partial

from effect import Effect, FuncIntent, Constant

from twisted.internet.task import deferLater
from twisted.python.failure import Failure

from otter.util.pure_http import (
    get_request, request_with_auth, check_status)
from otter.util.http import headers
from otter.util.retry import compose_retries, retry_times


def get_request_func(authenticator, tenant_id, log):
    """
    Return a pure_http.Request-returning function extended with:

    - authentication for Rackspace APIs
    - HTTP status code checking
    - JSON bodies and return values
    - returning only content of the result, not response objects
    - logging
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


def should_retry(clock, can_retry, next_interval, e):
    """
    Determine whether an HTTP request should be retried.

    After the first three parameters are curried, this function is appropriate
    to pass as the 'should_retry' parameter to :func:`effect.retry.retry`.

    :param clock: a reactor
    :param can_retry: a pure function of Failure -> bool that indicates whether
        a retry should be performed.
    :param next_interval: a potentially impure function that returns an
        interval to wait for the next retry attempt.
    :param tuple e: an exception tuple
    """
    # - limit number of retries
    # - exponentially back-off in time
    # - consider the response code? need use cases.
    #   - metrics retries on any API error.
    #   - most likely *any* GET request retries on API error,
    #   - ... unless the caller wants to know about a 404.
    times = retry_times(5)
    if can_retry:
        can_retry = compose_retries(times, can_retry)
    else:
        can_retry = times

    if can_retry(Failure(e[1], e[0], e[2])):
        # ok, we can retry! sleep for an interval first.
        later = lambda: deferLater(clock, next_interval(), lambda: True)
        return Effect(FuncIntent(later))
    else:
        return Effect(Constant(False))
