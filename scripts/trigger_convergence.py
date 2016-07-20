#!/usr/bin/env python

"""
Trigger convergence on all or specific tenants/groups. Requires otter type
config file containing region, identity and cassandra info

Examples:
`python trigger_convergence -c config.json -g "tenantid:groupId"`
will trigger convergence on given group(s)
`python trigger_convergence -c config.json --all`
will trigger convergence on all groups got from cassandra
"""

from __future__ import print_function

import json
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from functools import partial
from pprint import pprint

from effect import Effect, Func, parallel
from effect.do import do, do_return

from toolz.curried import filter
from toolz.dicttoolz import assoc
from toolz.itertoolz import concat
from toolz.recipes import countby

import treq

from twisted.internet import task
from twisted.internet.defer import (
    DeferredList, DeferredSemaphore, gatherResults, inlineCallbacks, succeed)

from txeffect import perform

from otter.auth import generate_authenticator, public_endpoint_url
from otter.cloud_client import TenantScope
from otter.constants import get_service_configs
from otter.convergence.gathering import get_all_launch_server_data
from otter.convergence.planning import Destiny, get_destiny
from otter.convergence.service import convergence_exec_data, get_executor
from otter.effect_dispatcher import get_full_dispatcher
from otter.metrics import connect_cass_servers
from otter.models.cass import CassScalingGroupCollection, DEFAULT_CONSISTENCY
from otter.test.utils import mock_log
from otter.util.config import set_config_data
from otter.util.cqlbatch import batch
from otter.util.http import append_segments, check_success, headers
from otter.util.timestamp import datetime_to_epoch


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
    yield check_success(resp, [204])


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
    d = DeferredList(
        [sem.run(trigger_convergence, authenticator, region, group,
                 no_error_group)
         for group in groups],
        fireOnOneCallback=False,
        fireOnOneErrback=False,
        consumeErrors=True)
    d.addCallback(
        lambda results: [(g["tenantId"], g["groupId"], f.value)
                         for g, (s, f) in zip(groups, results) if not s])
    return d


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
        d = store.get_all_valid_groups()
    elif parsed.tenant_id:
        d = get_groups_of_tenants(log, store, parsed.tenant_id)
    elif parsed.disabled_tenants:
        non_conv_tenants = conf["non-convergence-tenants"]
        d = store.get_all_valid_groups()
        d.addCallback(
            filter(lambda g: g["tenantId"] not in set(non_conv_tenants)))
        d.addCallback(list)
    elif parsed.conf_conv_tenants:
        d = get_groups_of_tenants(log, store, conf["convergence-tenants"])
    else:
        raise SystemExit("Unexpected group selection")
    return d


def active_servers_count(servers):
    """
    Return number of active servers based on their destiny.

    :param list servers: List of :obj:`NovaServer`

    :return: Number of servers as ``int``
    """
    counts = defaultdict(lambda: 0)
    counts.update(countby(get_destiny, servers))
    return (counts[Destiny.CONSIDER_AVAILABLE] +
            counts[Destiny.AVOID_REPLACING] +
            counts[Destiny.WAIT] +
            counts[Destiny.WAIT_WITH_TIMEOUT])


@do
def group_steps(group):
    """
    Return Effect of list of steps that would be performed on the group
    if convergence is triggered on it with desired=actual. Also returns
    current delta of desired and actual
    """
    now_dt = yield Effect(Func(datetime.utcnow))
    all_data_eff = convergence_exec_data(
        group["tenantId"], group["groupId"], now_dt, get_executor)
    try:
        all_data = yield Effect(TenantScope(all_data_eff, group["tenantId"]))
    except Exception as e:
        yield do_return((e, 0))
    (executor, scaling_group, group_state, desired_group_state,
     resources) = all_data
    active_servers = active_servers_count(resources['servers'])
    delta = desired_group_state.capacity - active_servers
    desired_group_state.capacity = active_servers
    steps = executor.plan(desired_group_state, datetime_to_epoch(now_dt),
                          3600, {}, **resources)
    yield do_return((steps, desired_group_state.capacity, delta))


def groups_steps(groups, reactor, store, cass_client, authenticator, conf):
    """
    Return [(group, (steps, delta))] list
    """
    eff = parallel(map(group_steps, groups))
    from twisted.python import log as tlog
    from otter.log import log as otter_log
    from otter.log.setup import observer_factory
    tlog.startLoggingWithObserver(observer_factory())
    disp = get_full_dispatcher(
        reactor, authenticator, otter_log, get_service_configs(conf),
        "kzclient", store, "supervisor", cass_client)
    d = perform(disp, eff)
    return d.addCallback(lambda steps: zip(groups, steps))


@inlineCallbacks
def set_desired_to_actual_group(dispatcher, cass_client, group):
    """
    Set group's desired to current number of servers in the group
    """
    try:
        res_eff = get_all_launch_server_data(
            group["tenantId"], group["groupId"], datetime.utcnow())
        eff = Effect(TenantScope(res_eff, group["tenantId"]))
        resources = yield perform(dispatcher, eff)
        actual = active_servers_count(resources["servers"])
        print("group", group, "setting desired to ", actual)
        yield cass_client.execute(
            ('UPDATE scaling_group SET desired=:desired WHERE '
             '"tenantId"=:tenantId AND "groupId"=:groupId'),
            assoc(group, "desired", actual), DEFAULT_CONSISTENCY)
    except Exception as e:
        print("Couldn't set group {} to {} due to {}".format(group, actual, e))


def set_desired_to_actual(groups, reactor, store, cass_client, authenticator,
                          conf):
    dispatcher = get_full_dispatcher(
        reactor, authenticator, mock_log(), get_service_configs(conf),
        "kzclient", store, "supervisor", cass_client)
    return gatherResults(
        map(partial(set_desired_to_actual_group, dispatcher, cass_client),
            groups))


def pause_groups(cass_client, groups):
    """
    Pause given groups by setting their paused=true
    """
    queries, params = [], {}
    for i, group in enumerate(groups):
        queries.append(
            ('UPDATE scaling_group SET paused=true WHERE '
             '"tenantId"=:tenantId{0} AND "groupId"=:groupId{0}').format(i))
        params["tenantId{}".format(i)] = group["tenantId"]
        params["groupId{}".format(i)] = group["groupId"]
    return cass_client.execute(batch(queries), params, DEFAULT_CONSISTENCY)


@inlineCallbacks
def main(reactor):
    parser = ArgumentParser(
        description="Trigger convergence on all/some groups")
    parser.add_argument(
        "-c", dest="config", required=True,
        help="Config file containing identity and cassandra info")
    parser.add_argument(
        "--steps", action="store_true",
        help=("Return steps that would be taken if convergence was triggered "
              "with desired set to current actual. No convergence triggered"))
    parser.add_argument(
        "--set-desired-to-actual", action="store_true", dest="set_desired",
        help="Set group's desired to current actual number of servers")
    parser.add_argument(
        "--pause", action="store_true", dest="pause_group",
        help="Pause given groups")

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

    set_config_data(conf)
    cass_client = connect_cass_servers(reactor, conf["cassandra"])
    authenticator = generate_authenticator(reactor, conf["identity"])
    store = CassScalingGroupCollection(cass_client, reactor, 1000)

    groups = yield get_groups(parsed, store, conf)
    if parsed.steps:
        steps = yield groups_steps(groups, reactor, store, cass_client,
                                   authenticator, conf)
        pprint(steps)
    elif parsed.set_desired:
        yield set_desired_to_actual(groups, reactor, store, cass_client,
                                    authenticator, conf)
    elif parsed.pause_group:
        yield pause_groups(cass_client, groups)
    else:
        error_groups = yield trigger_convergence_groups(
            authenticator, conf["region"], groups, parsed.limit,
            parsed.no_error_group)
        if error_groups:
            print("Following groups errored")
            pprint(error_groups)
    yield cass_client.disconnect()


if __name__ == '__main__':
    task.react(main, ())
