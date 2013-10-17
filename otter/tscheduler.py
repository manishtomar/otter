#!/usr/bin/env python

from __future__ import print_function

from datetime import datetime
import uuid
import sys
import os
import random

from twisted.internet import defer, task
from twisted.internet.endpoints import clientFromString

import iso8601
import zc.lockfile

from silverberg.cassandra.ttypes import ConsistencyLevel
from silverberg.cluster import RoundRobinCassandraCluster
from silverberg.lock import BasicLock, with_lock

from cql.connection import connect


_cql_fetch_batch_of_events = (
    'SELECT "tenantId", "groupId", "policyId", "trigger", cron FROM sample_events WHERE '
    'trigger = :now LIMIT :size;')
_cql_delete_event = ('DELETE FROM sample_events '
                     'WHERE trigger = :trigger AND "policyId" = :{policy_id}')
_cql_insert_event_with_cron = ('INSERT INTO sample_events("tenantId", "groupId", "policyId", '
                               'trigger, cron) '
                               'VALUES (:tenantId, :groupId, :{name}Id, '
                               ':{name}Trigger, :{name}cron); ')


class Batch(object):
    """ CQL Batch wrapper"""
    def __init__(self, statements, params, consistency=ConsistencyLevel.ONE,
                 timestamp=None):
        self.statements = statements
        self.params = params
        self.consistency = consistency
        self.timestamp = timestamp

    def _generate(self):
        str = 'BEGIN BATCH '
        if self.timestamp is not None:
            str += 'USING TIMESTAMP {} '.format(self.timestamp)
        str += ' '.join(self.statements)
        str += ' APPLY BATCH;'
        return str

    def execute(self, client):
        """
        Execute the CQL batch against the given client object
        """
        return client.execute(self._generate(), self.params, self.consistency)


class FileLock(object):
    def __init__(self, fname, max_retry=0, retry_wait=10, clock=None):
        self.fname = fname
        self.max_retry = max_retry
        self.retry_wait = retry_wait
        self._lock = None
        self.clock = clock

    def acquire(self):

        pid = os.getpid()

        def _try_lock(i):
            try:
                print('trying', pid, i)
                self._lock = zc.lockfile.LockFile(self.fname)
            except zc.lockfile.LockError:
                if i + 1 > self.max_retry:
                    raise
                return task.deferLater(self.clock, self.retry_wait, _try_lock, i + 1)

        return _try_lock(0)

    def release(self):
        self._lock.close()

    def with_lock(self, func, *args, **kwargs):

        d = defer.maybeDeferred(self.acquire)

        def release_lock(result):
            self.release()
            return result

        def lock_acquired(_):
            return defer.maybeDeferred(func, *args, **kwargs).addBoth(release_lock)

        d.addCallback(lock_acquired)
        return d


def waiting_deferred(reactor, delay, result):
    d = defer.Deferred()
    reactor.callLater(delay, d.callback, result)
    return d


def test_file_lock(reactor, client, args):
    fname, times, pid = args[0], int(args[1]), os.getpid()
    lock = FileLock(fname, max_retry=30, retry_wait=random.uniform(1, 3), clock=reactor)

    def _in(i):
        print('acquired lock', pid, i)
        d = waiting_deferred(reactor, random.uniform(0, 2), '')
        d.addCallback(print, 'released lock', pid, i)
        return d

    def _get_lock(i):
        d = lock.with_lock(_in, i)
        return d.addCallback(lambda _: waiting_deferred(reactor, 0.2, ''))

    return task.coiterate((_get_lock(i) for i in range(0, times)))


def print_with_time(r, reactor, start, msg, result=True, *args):
    seconds = reactor.seconds() - start
    if result:
        print(msg + ' with result {} completed in {}'.format(r, seconds), *args)
    else:
        print(msg + ' completed in {}'.format(seconds), *args)
    return r


def fetch_and_delete(reactor, client, now, size=100):
    """
    Fetch events to be occurring now and delete them after fetching
    """
    def _fetch_and_delete(now, size):
        d = client.execute(_cql_fetch_batch_of_events,
                           {"size": size, "now": now},
                           ConsistencyLevel.QUORUM)
        return d.addCallback(delete_events).addCallback(
            print_with_time, reactor, reactor.seconds(), 'fetch/del', result=False)

    def delete_events(events):
        if not events:
            return events
        print('Got events', len(events))
        data = {'trigger': now}
        queries = []
        for i, event in enumerate(events):
            policy_id = event['policyId']
            pol_num = 'policyid{}'.format(i)
            queries.append(_cql_delete_event.format(policy_id=pol_num))
            data[pol_num] = policy_id
        d = Batch(queries, data, ConsistencyLevel.QUORUM).execute(client)
        return d.addCallback(lambda _: events)

    lock = BasicLock('locks', 'schedule', max_retry=30, retry_wait=random.uniform(0.5, 1.5),
                     clock=reactor)
    return with_lock(lock, _fetch_and_delete, now, size)
    #lock = FileLock('schedule', max_retry=30, retry_wait=random.uniform(0.5, 1.5),
    #                clock=reactor)
    #return lock.with_lock(_fetch_and_delete, now, size)


def scheduler(reactor, client, args):
    """
    Check for events in the database before the present time.

    :return: a deferred that fires with None
    """

    trigger = iso8601.parse_date(args[0])
    print('trigger', trigger)
    batchsize = 100
    total = [0]

    def process_events(events, trigger):
        print("processing {} events".format(len(events)))
        total[0] += len(events)
        d = defer.Deferred()
        reactor.callLater(random.uniform(0.1, 0.5), d.callback, events)
        return d

    def check_for_more(events):
        if events and len(events) == batchsize:
            return _do_check()
        return None

    def _do_check():
        d = fetch_and_delete(reactor, client, trigger, batchsize)
        #d.addErrback(ignore_and_log, BusyLockError, self.log,
        #             "Couldn't get lock to fetch events")
        d.addCallback(process_events, trigger)
        d.addCallback(check_for_more)
        #d.addErrback(print, 'Error')
        return d

    return _do_check().addCallback(
        print_with_time, reactor, reactor.seconds(), 'scheduler total', False, total)


def insert(reactor, client, args):
    """
    Insert sample events in cass
    """
    trigger = iso8601.parse_date(args[0])
    num = int(args[1])
    print('trigger, num', trigger, num)

    def _insert_events(n):
        if not n:
            return
        batch = 'BEGIN BATCH '
        data = {'tenantId': str(random.uniform(30000, 40000)),
                'groupId': str(uuid.uuid4())}
        for i in range(n):
            name = 'policy{}'.format(i)
            batch += _cql_insert_event_with_cron.format(name=name)
            data.update({name + 'Id': str(uuid.uuid4()),
                         name + 'Trigger': trigger,
                         name + 'cron': '*'})
        batch += ' APPLY BATCH;'
        #print('batch, data', batch, data)
        return client.execute(batch, data, ConsistencyLevel.ONE)

    def _insert_all(total):
        block = 100
        for _ in range(0, total / block):
            yield _insert_events(block)
        yield _insert_events(total % block)
        #print('inserted {} events in {} seconds'.format(total, reactor.seconds() - seconds))

    return task.coiterate(_insert_all(num))
    #return task.coiterate(_insert_all(num)).addCallback(
    #    print_with_time, reactor, reactor.seconds(), 'inserted {} events'.format(total))


def delete(reactor, client, args):
    return client.execute(
        'DELETE FROM sample_events WHERE trigger=:trigger;',
        {'trigger': iso8601.parse_date(args[0])}, ConsistencyLevel.ONE)


def schema(clients):
    """
    Build keyspace and tables
    """
    schema = """
    CREATE KEYSPACE manitest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};
    USE manitest;
    CREATE TABLE sample_events (
        "tenantId" ascii,
        "groupId" ascii,
        "policyId" ascii,
        trigger timestamp,
        cron ascii,
        PRIMARY KEY(trigger, "policyId")
    ) WITH compaction = {
        'class' : 'SizeTieredCompactionStrategy',
        'min_threshold' : '2'
    } AND gc_grace_seconds = 3600;
    CREATE TABLE locks (
        "lockId" ascii,
        "claimId" timeuuid,
        PRIMARY KEY ("lockId", "claimId")
    ) WITH compaction = {
        'class' : 'SizeTieredCompactionStrategy',
        'min_threshold' : '2'
    } AND gc_grace_seconds = 3600;
    """
    for client in clients:
        _, host, port = client.split(':')
        print('Connecting to {}:{}'.format(host, port))
        conn = connect(host, port, cql_version='3.0.4')
        c = conn.cursor()
        for query in schema.split(';'):
            query = query.strip()
            if query:
                print('Executing', query)
                c.execute(query, {})


def nothing(reactor, client, args):
    query = ('SELECT "tenantId", "groupId", "policyId", "trigger", cron FROM sample_events WHERE '
            'trigger = :now LIMIT :size;')
    query = _cql_fetch_batch_of_events
    return client.execute(
        query, {'now': iso8601.parse_date(args[0]), 'size': 10},
        ConsistencyLevel.ONE)


def main(reactor, *args):
    clients = os.getenv('CASS_CLIENTS').split(',')
    print('cass clients', clients)
    hosts = [clientFromString(reactor, client) for client in clients]
    client = RoundRobinCassandraCluster(hosts, 'manitest')
    start_seconds = reactor.seconds()
    if args[0] == 'schema':
        return schema(clients)
    d = getattr(sys.modules[__name__], args[0])(reactor, client, args[1:])
    d.addBoth(print_with_time, reactor, reactor.seconds(), args[0]).addCallback(
        lambda _: client.disconnect())
    return d

task.react(main, sys.argv[1:])

