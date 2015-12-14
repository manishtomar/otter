#!/usr/bin/env python

"""
Trigger convergence on all or specific tenants/groups. Requires otter type
config file containing region, identity and cassandra info

Examples:
`python trigger_convergence -c config.json -g "tenantid:groupId"`
will trigger convergence on given group(s)
`python trigger_convergence -c config.json`
will trigger convergence on all groups got from cassandra
"""

from __future__ import print_function

import json
from argparse import ArgumentParser

from toolz.curried import keyfilter
from toolz.itertoolz import concat

import treq

from twisted.internet import task
from twisted.internet.defer import (
    DeferredSemaphore, gatherResults, inlineCallbacks, succeed)

from otter.auth import generate_authenticator, public_endpoint_url
from otter.metrics import connect_cass_servers
from otter.models.cass import CassScalingGroupCollection
from otter.test.utils import mock_log
from otter.util.http import append_segments, headers


@inlineCallbacks
def trigger_convergence(authenticator, region, group, no_error_group):
    """
    Trigger convergence on a group

    :param IAuthenticator authenticator: Otter authenticator
    :param str region: Region where this is running
    :param dict group: Scaling group dict
    :param bool no_error_group: If true then do not converge ERROR groups
    """
    token, catalog = yield authenticator.authenticate_tenant(group["tenantId"])
    endpoint = public_endpoint_url(catalog, "autoscale", region)
    conv_on_error = "false" if no_error_group else "true"
    resp = yield treq.post(
        append_segments(endpoint, "groups", group["groupId"], "converge"),
        headers=headers(token), params={"on_error": conv_on_error}, data="")
    if resp.code != 204:
        raise ValueError("bad code", resp.code)


def trigger_convergence_groups(authenticator, region, groups,
                               concurrency_limit, no_error_group):
    """
    Trigger convergence on given groups

    :param IAuthenticator authenticator: Otter authenticator
    :param str region: Region where this is running
    :param list groups: List of group dicts
    :param int concurrency_limit: Concurrency limit
    :param bool no_error_group: If true then do not converge ERROR groups

    :return: Deferred fired with None
    """
    sem = DeferredSemaphore(concurrency_limit)
    return gatherResults(
        [sem.run(trigger_convergence, authenticator, region, group,
                 no_error_group)
         for group in groups],
        consumeErrors=True).addCallback(lambda _: None)


def get_groups_of_tenants(log, store, tenant_ids):
    """
    Return groups of given list of tenants

    :param log: Twisted logger
    :param store: Otter scaling group collection
    :param list tenant_ids: List of tenants whose groups are required

    :return: Deferred fired with list of {"tenantId": .., "groupId": ..} dict
    """
    d = gatherResults([
        store.list_scaling_group_states(log, tenant_id)
        for tenant_id in tenant_ids])
    d.addCallback(concat)
    d.addCallback(lambda states: [{"tenantId": s.tenant_id,
                                   "groupId": s.group_id}
                                  for s in states])
    return d


def get_groups(parsed, store, conf):
    """
    Return groups based on argument provided

    :param Namespace parsed: arguments parsed
    :param store: Otter scaling group collection
    :param dict conf: config

    :return: Deferred fired with list of {"tenantId": .., "groupId": ..} dict
    """
    log = mock_log()
    if parsed.group:
        groups = [g.split(":") for g in parsed.group]
        return succeed(
            [{"tenantId": tid, "groupId": gid} for tid, gid in groups])
    elif parsed.all:
        d = store.get_all_groups()
        d.addCallback(lambda tgs: concat(tgs.values()))
    elif parsed.tenant_id:
        d = get_groups_of_tenants(log, store, parsed.tenant_id)
    elif parsed.disabled_tenants:
        non_conv_tenants = conf["non-convergence-tenants"]
        d = store.get_all_groups()
        d.addCallback(keyfilter(lambda k: k not in set(non_conv_tenants)))
        d.addCallback(lambda tgs: concat(tgs.values()))
    elif parsed.conf_conv_tenants:
        d = get_groups_of_tenants(log, store, conf["convergence-tenants"])
    else:
        raise SystemExit("Unexpected group selection")
    return d


@inlineCallbacks
def main(reactor):
    parser = ArgumentParser(
        description="Trigger convergence on all/some groups")
    parser.add_argument(
        "-c", dest="config", required=True,
        help="Config file containing identity and cassandra info")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-g", nargs="+", dest="group",
        help="Group(s) to trigger. Should be in tenantId:groupId form")
    group.add_argument(
        "-t", nargs="+", dest="tenant_id",
        help="TenantID(s) whose group's to trigger")
    group.add_argument(
        "--conf-conv-tenants", action="store_true",
        help=("Convergence triggered on tenants configured as "
              "\"convergence-tenants\" setting config file"))
    group.add_argument(
        "--conf-non-conv-tenants", action="store_true",
        dest="disabled_tenants",
        help=("Convergence triggered on all tenants except ones in "
              "\"non-convergence-tenants\" setting in conf file"))
    group.add_argument("--all", action="store_true",
                       help="Convergence will be triggered on all groups")

    parser.add_argument("-l", dest="limit", type=int, default=10,
                        help="Concurrency limit. Defaults to 10")
    parser.add_argument("--no-error-group", action="store_true",
                        help="Do not converge ERROR groups")

    parsed = parser.parse_args()
    conf = json.load(open(parsed.config))

    cass_client = connect_cass_servers(reactor, conf["cassandra"])
    authenticator = generate_authenticator(reactor, conf["identity"])
    store = CassScalingGroupCollection(cass_client, reactor, 1000)

    groups = yield get_groups(parsed, store, conf)
    print(*set(g["tenantId"] for g in groups), sep="\n")

    #yield trigger_convergence_groups(
    #    authenticator, conf["region"], groups, parsed.limit,
    #    parsed.no_error_group)
    yield cass_client.disconnect()


if __name__ == '__main__':
    task.react(main, ())
