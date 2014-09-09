"""
Purely functional HTTP client.
"""
import json

from functools import partial

from effect import Effect
from characteristic import attributes
from toolz.dicttoolz import merge
from twisted.internet.defer import inlineCallbacks, returnValue

from otter.util import logging_treq
from otter.util.http import APIError


@attributes(['method', 'url', 'headers', 'data', 'log'],
            defaults={'headers': None, 'data': None, 'log': None})
class Request(object):
    """
    An Effect intent for representing HTTP requests.

    The effect results in a two-tuple of (response, content).
    """

    treq = logging_treq

    @inlineCallbacks
    def perform_effect(self, dispatcher):
        """
        Perform the request with treq.

        :return: A two-tuple of (HTTP Response, content as bytes)
        """
        response = yield self.treq.request(self.method.upper(), self.url,
                                           headers=self.headers,
                                           data=self.data, log=self.log)
        content = yield self.treq.content(response)
        returnValue((response, content))


def get_request(method, url, **kwargs):
    """Return a Request wrapped in an Effect."""
    return Effect(Request(method=method, url=url, **kwargs))


def auth_request(get_request, method, url, get_auth_headers, headers=None,
                 **kwargs):
    """
    Performs an authenticated request, calling a function to get auth headers.

    :param get_auth_headers: A function that should return an Effect that
        returns auth-related headers as a dict.
    """
    def try_request(auth_headers):
        req_headers = {} if headers is None else headers
        req_headers = merge(req_headers, auth_headers)
        eff = get_request(method, url, headers=req_headers, **kwargs)
        return eff
    return get_auth_headers().on(success=try_request)


def refresh_auth_on_error(reauth_codes, refresh_auth_info, result):
    """
    Refreshes an auth cache if an HTTP response is an auth-related error.

    :param refresh_auth_info: A function that should return an Effect that
        invalidates or clears out any cached auth information that
        auth_request's get_auth_headers function returns.
    :param tuple reauth_codes: integer HTTP codes which should cause a refresh.
    """
    response, content = result
    if response.code in reauth_codes:
        return refresh_auth_info().on(success=lambda ignored: result)
    else:
        return result


def request_with_auth(get_request, method, url,
                      get_auth_headers,
                      refresh_auth_info,
                      headers=None, reauth_codes=(401, 403),
                      **kwargs):
    """
    Get a request that will perform book-keeping on cached auth info.

    This composes the :func:`auth_request` and :func:`refresh_auth_on_error`
    functions.

    :param get_auth_headers: As per :func:`auth_request`
    :param refresh_auth_info: As per :func:`refresh_auth_on_error`
    :param reauth_codes: As per :func:`refresh_auth_on_error`.
    """
    eff = auth_request(get_request, method, url, get_auth_headers, headers=headers, **kwargs)
    return eff.on(success=partial(refresh_auth_on_error, reauth_codes, refresh_auth_info))


def status_check(success_codes, result):
    """Ensure that the response code is acceptable. If not, raise APIError."""
    (response, content) = result
    if response.code not in success_codes:
        raise APIError(response.code, content, response.headers)
    return result


def request_with_status_check(get_request, method, url, success_codes=(200,),
                              **kwargs):
    """Make a request and perform a status check on the response."""
    return get_request(method, url, **kwargs).on(
        success=partial(status_check, success_codes))


def request_with_json(get_request, method, url, data=None, **kwargs):
    """Convert the request body to JSON, and parse the response as JSON."""
    if data is not None:
        data = json.dumps(data)
    return get_request(method, url, data=data, **kwargs).on(
        success=lambda r: (r[0], json.loads(r[1])))


def content_request(effect):
    """Only return the content part of a response."""
    return effect.on(success=lambda r: r[1])
