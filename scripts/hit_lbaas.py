"""
Test to smash lbaas
"""
from __future__ import print_function

from random import choice, shuffle
import os
import mock
import json

from twisted.internet import defer, reactor
from twisted.internet.task import react, Cooperator, deferLater
import treq

from otter import auth
from otter.util import http, config
from otter.util.retry import retry, retry_times, repeating_interval
from otter.log import log
from otter.worker.launch_server_v1 import (
    add_to_load_balancer, remove_from_load_balancer)


username = os.getenv('LBAAS_USERNAME')
password = os.getenv('LBAAS_PASSWORD')
auth_endpoint = "https://staging.identity.api.rackspacecloud.com/v2.0"
lb_endpoint = "https://staging.ord.loadbalancers.api.rackspacecloud.com/v1.0"

config.set_config_data(
    {'worker': {'lb_max_retries': 100, 'lb_retry_interval': 1}})


def make_loadbalancer(tenant, auth_token):
    d = treq.post(http.append_segments(lb_endpoint, tenant, 'loadbalancers'),
                  headers=http.headers(auth_token), data=json.dumps(
                    {
                        "loadBalancer": {
                            "name": "a-new-loadbalancer",
                            "port": 80,
                            "protocol": "HTTP",
                            "virtualIps": [
                                {
                                    "type": "PUBLIC"
                                }
                            ],
                            "nodes": [
                                {
                                    "address": "10.1.1.1",
                                    "port": 80,
                                    "condition": "ENABLED"
                                }
                            ]
                        }
                    }))
    d.addCallback(http.check_success, [202])
    d.addCallback(treq.json_content)
    d.addCallback(lambda result: str(result['loadBalancer']['id']))
    return d


def del_loadbalancer(lb_id, tenant, auth_token):
    print(lb_id)
    d = treq.delete(
        http.append_segments(lb_endpoint, tenant, 'loadbalancers', lb_id),
        headers=http.headers(auth_token))
    return d


def retry_delete(node_id, ip, lb_id, tenant, auth_token):
    def delete():
        return remove_from_load_balancer(
            log, http.append_segments(lb_endpoint, tenant), auth_token, lb_id,
            node_id)

    d = retry(
        delete,
        can_retry=retry_times(config.config_value('worker.lb_max_retries')),
        next_interval=repeating_interval(
            config.config_value('worker.lb_retry_interval')))

    d.addCallback(debug, '{0} deleted from {1}'.format(ip, lb_id))
    d.addErrback(debug)
    return d


def retry_add(ip, lb_id, tenant, auth_token):
    d = add_to_load_balancer(log, http.append_segments(lb_endpoint, tenant),
                             auth_token, {'loadBalancerId': lb_id, 'port': 80},
                             ip, mock.Mock())
    d.addCallback(debug, "{0} added to {1}".format(ip, lb_id))
    d.addCallback(lambda result: (result['nodes'][0]['id'],
                                  result['nodes'][0]['address']))
    d.addErrback(debug)
    return d


def add_and_delete_one(ip, lb_id, tenant, auth_token):
    d = retry_add(ip, lb_id, tenant, auth_token)
    d.addCallback(retry_delete, ip, lb_id, tenant, auth_token)
    d.addErrback(print)
    d.addCallback(lambda _: lb_id)
    return d


def harass_lb_one_at_a_time(lb_id, tenant, auth_token):
    """
    The workflow is to add a node (with retrying), then delete it from the LB
    (with retrying) once it's been added.

    This is done in batches of 50 at a time, for 254 nodes.
    """
    coop = Cooperator()
    work = (add_and_delete_one(ip, lb_id, tenant, auth_token) for ip in
            ('10.1.1.{0}'.format(i) for i in xrange(2, 256)))
    d = defer.DeferredList([coop.coiterate(work) for i in xrange(50)])
    d.addCallback(lambda _: lb_id)
    return d


def add_and_delete_multiple(ips, lb_id, tenant, auth_token):
    def delete(info):
        return defer.DeferredList(
            [retry_delete(result[0], result[1], lb_id, tenant, auth_token)
             for success, result in info if success],
            consumeErrors=True)

    d = defer.DeferredList([deferLater(reactor, i * 1.1, retry_add, ip, lb_id,
                                       tenant, auth_token)
                            for i, ip in enumerate(ips)], consumeErrors=True)
    d.addCallback(delete)
    return d


def harass_lb_24_at_a_time(lb_id, tenant, auth_token):
    """
    The workflow is to add 24 nodes (each with retrying) at a time, then delete
    all 24 (each with retrying) once all have been added.

    This is done for 254 nodes.
    """
    def chunks_24():  # get 24 ips at a time
        for i in xrange(2, 256, 29):
            yield ['10.1.1.{0}'.format(j) for j in range(i, i + 29)
                   if j < 256]

    coop = Cooperator()
    work = (add_and_delete_multiple(ips, lb_id, tenant, auth_token)
            for ips in chunks_24())
    return coop.coiterate(work)


def harass_lb_with_mostly_garbage(lb_id, tenant, auth_token):
    """
    The workflow is just to attempt to submit garbage node info to the delete
    endpoint over and over.

    This is done in batches of 50, for 508 nodes.
    """
    good_ips = ['10.1.{0}.{1}'.format(i, j)
                for i in (1, 2) for j in range(50, 256, 50)]

    d = defer.DeferredList([retry_add(ip, lb_id, tenant, auth_token)
                            for ip in good_ips])

    def garbage():  # produce garbage data
        a = list('0123456')
        match = {
            '{': '}',
            '(': ')',
            '[': ']'
        }
        for i in range(1, 3):
            for j in xrange(2, 256):
                ip = '10.1.{0}.{1}'.format(i, j)
                if ip in good_ips:
                    yield ip
                else:
                    start = choice(match.keys())
                    shuffle(a)
                    yield start + ip + ', ' + ''.join(a) + match[start]

    coop = Cooperator()
    work = (retry_delete(g, g, lb_id, tenant, auth_token)
            for g in garbage())

    d.addCallback(lambda _:
        defer.DeferredList([coop.coiterate(work) for i in xrange(50)]))
    d.addCallback(lambda _: lb_id)
    return d


def get_auth_token():
    d = auth.authenticate_user(auth_endpoint, username, password, log)
    return d.addCallback(
        lambda result: (
            result['access']['token']['tenant']['id'],
            result['access']['token']['id'].encode('ascii')))


def debug(stuff, text=None):
    if text is not None:
        print(text)
    else:
        print(stuff)
    return stuff


def do_stuff((tenant, auth_token)):
    d = make_loadbalancer(tenant, auth_token)
    d.addCallback(debug)
    # d.addCallback(harass_lb_one_at_a_time, tenant, auth_token)
    # d.addCallback(harass_lb_24_at_a_time, tenant, auth_token)
    d.addCallback(harass_lb_with_mostly_garbage, tenant, auth_token)
    d.addCallback(del_loadbalancer, tenant, auth_token)
    return d


def cleanup_loadbalancers((tenant, auth_token)):
    path = http.append_segments(lb_endpoint, tenant, 'loadbalancers')
    headers = http.headers(auth_token)

    d = treq.get(path, headers=headers)
    d.addCallback(http.check_success, [200])
    d.addCallback(treq.json_content)
    d.addCallback(lambda result: [str(lb['id']) for lb in result['loadBalancers']])
    d.addCallback(debug)
    d.addCallback(lambda ids: defer.DeferredList([
        treq.delete(http.append_segments(path, i), headers=headers)
        .addCallback(http.check_success, [202])
        .addCallback(treq.content)

        for i in ids], consumeErrors=True))
    d.addCallback(debug)
    return d


def go(_):
    d = get_auth_token()
    #d.addCallback(cleanup_loadbalancers)
    d.addCallback(do_stuff)
    return d


if __name__ == "__main__":
    react(go, [])
