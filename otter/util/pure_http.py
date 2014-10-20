"""
Purely functional HTTP client.
"""
from functools import partial, wraps

from effect import Effect
from characteristic import attributes
from toolz.dicttoolz import merge
from toolz.functoolz import compose
from twisted.internet.defer import inlineCallbacks, returnValue

from otter.util import logging_treq
from otter.util.fp import wrappers
from otter.util.http import APIError


@attributes(['method', 'url', 'headers', 'data', 'log'],
            defaults={'headers': None, 'data': None, 'log': None})
class Request(object):
    """
    An effect request for performing HTTP requests.

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


def auth_request(get_request, get_auth_headers, headers=None):
    """
    Performs an authenticated request, calling a function to get auth headers.

    :param get_request: A function which only accepts a 'headers' argument.
    :param get_auth_headers: An Effect that returns auth-related headers as a dict.
    """
    headers = headers if headers is not None else {}
    return get_auth_headers.on(
        success=lambda auth_headers: get_request(merge(headers, auth_headers)))


def refresh_auth_on_error(reauth_codes, refresh_auth_info, result):
    """
    Refreshes an auth cache if an HTTP response is an auth-related error.

    :param refresh_auth_info: An Effect that invalidates or clears out any
        cached auth information that auth_request's get_auth_headers function
        returns.
    :param tuple reauth_codes: integer HTTP codes which should cause a refresh.
    """
    response, content = result
    if response.code in reauth_codes:
        return refresh_auth_info.on(success=lambda ignored: result)
    else:
        return result


def request_with_auth(get_request,
                      get_auth_headers,
                      refresh_auth_info,
                      headers=None, reauth_codes=(401, 403)):
    """
    Get a request that will perform book-keeping on cached auth info.

    This composes the :func:`auth_request` and :func:`refresh_auth_on_error`
    functions.

    :param get_auth_headers: As per :func:`auth_request`
    :param refresh_auth_info: As per :func:`refresh_auth_on_error`
    :param reauth_codes: As per :func:`refresh_auth_on_error`.
    """
    if reauth_codes is None:
        import pdb; pdb.set_trace()
    eff = auth_request(get_request, get_auth_headers, headers=headers)
    return eff.on(success=partial(refresh_auth_on_error, reauth_codes,
                                  refresh_auth_info))


def check_status(success_codes, result):
    """Ensure that the response code is acceptable. If not, raise APIError."""
    (response, content) = result
    if response.code not in success_codes:
        raise APIError(response.code, content, response.headers)
    return result


def bind_root(request_func, root):
    """
    Given a request function, return a new request function that only takes a
    relative path instead of an absolute URL.
    """
    @wraps(request_func)
    def request(method, url, *args, **kwargs):
        return request_func(method, root + url, *args, **kwargs)
    return request
