#!/usr/bin/env python

from __future__ import print_function

from datetime import datetime
import uuid
import sys
import os
import random

from twisted.internet import defer, task
from twisted.internet.endpoints import clientFromString
from twisted.python import log
from twisted.python.failure import Failure

import iso8601
import zc.lockfile

from silverberg.cassandra.ttypes import ConsistencyLevel
from silverberg.cluster import RoundRobinCassandraCluster
#from silverberg.lock import BasicLock, with_lock

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


class LoggingCQLClient(object):
    """
    A logging CQL client. Every query will be timed and logged

    :param client: A `CQLClient` or `RoundRobinCassandraCluster` instance
    :param log: A bound logger that has .msg() method
    """

    def __init__(self, client, log, clock=None):
        self._client = client
        self._log = log
        if clock:
            self._clock = clock
        else:
            from twisted.internet import reactor
            self._clock = reactor

    def execute(self, query, args, consistency):
        """
        See :py:func:`silverberg.client.CQLClient.execute`
        """
        start_seconds = self._clock.seconds()

        def record_time(result):
            seconds_taken = self._clock.seconds() - start_seconds
            kwargs = dict(query=query, data=args, consistency=consistency,
                          seconds_taken=seconds_taken)
            if isinstance(result, Failure):
                self._log.msg('query {} failed. time: {}'.format(query, seconds_taken),
                              reason=result, **kwargs)
            else:
                self._log.msg('query {} success. time: {}'.format(query, seconds_taken), **kwargs)
            return result

        return self._client.execute(query, args, consistency).addBoth(record_time)

    def disconnect(self):
        self._client.disconnect()


class Lock(object):
    """
    Base lock for lock implementations
    """
    def acquire(self):
        """ Acquire the lock """
        pass

    def release(self):
        """ release the lock """
        pass

    def with_lock(self, func, *args, **kwargs):
        """ Context manager for the lock """

        d = defer.maybeDeferred(self.acquire)

        def release_lock(result):
            d = defer.maybeDeferred(self.release)
            return d.addCallback(lambda _: result)

        def lock_acquired(_):
            return defer.maybeDeferred(func, *args, **kwargs).addBoth(release_lock)

        d.addCallback(lock_acquired)
        return d


class BusyLockError(Exception):
    def __init__(self, lock_table, lock_id):
        super(BusyLockError, self).__init__(
            "Unable to acquire lock {id} on {table}".format(id=lock_id,
                                                            table=lock_table))


class NoLockClaimsError(Exception):
    def __init__(self, lock_table, lock_id):
        super(NoLockClaimsError, self).__init__(
            "No claims were found for lock {id} on {table}".format(
                id=lock_id,
                table=lock_table))


class BasicLock(Lock):
    """A locking mechanism for Cassandra.

    Based on the lock implementation from Netflix's astyanax, the lock recipe
    is a write, read, write operation. A record is written to the specified
    Cassandra database table with a timeuuid, and then the table is read for
    the given lock, ordered by timeuuid. If the first row is not ours, the
    lock was not acquired, so a write to remove the lock is made.
    Due to possibly timing issue in Cass, sometimes delete row to remove the lock
    does not take effect causing lock to be held until TTL (normally 5 mins) expires.
    Currently, mitigating this problem by having low ttl (normally 3 seconds) and inserting lock
    row every 1 second (in effect claiming the lock)

    :param client: A Cassandra CQL client
    :type client: silverberg.client.CQLClient

    :param lock_table: A table/columnfamily table name for holding locks.
    :type lock_table: str

    :param lock_id: A unique identifier for the lock.
    :type lock_id: str

    :param ttl: A TTL for the lock.
    :type ttl: int

    :param claim_interval: Interval in seconds to keep reclaiming the lock
    :type claim_interval: float

    :param max_retry: A number of times to retry acquisition of the lock.
    :type max_retry: int

    :param retry_wait: A number of seconds to wait before retrying acquisition.
    :type retry_wait: int

    :param reactor: A twisted clock.
    :type reactor: twisted.internet.interfaces.IReactorTime
    """

    def __init__(self, client, lock_table, lock_id, ttl=3, claim_interval=1,
                 max_retry=0, retry_wait=10, reactor=None, log=None):
        self._client = client
        self._lock_table = lock_table
        self._lock_id = lock_id
        #self._claim_id = uuid.uuid4()
        self._claim_id = uuid.uuid1()
        self._ttl = ttl
        self._claim_interval = claim_interval
        self._loop = None
        self._max_retry = max_retry
        self._retry_wait = retry_wait
        if reactor is None:
            from twisted.internet import reactor
        self._reactor = reactor

        self._log = log
        self._log_kwargs = dict(lock_id=self._lock_id, claim_id=self._claim_id)
        self._lock_acquired_seconds = None
        self._acquire_start_seconds = None

    def _read_lock(self, ignored):
        query = 'SELECT * FROM {cf} WHERE "lockId"=:lockId ORDER BY "claimId";'
        return self._client.execute(query.format(cf=self._lock_table),
                                    {'lockId': self._lock_id}, ConsistencyLevel.QUORUM)

    def _read_lock_trans(self, ignored):
        query = 'SELECT claimId FROM {cf} WHERE "lockId"=:lockId;'
        return self._client.execute(query.format(cf=self._lock_table),
                                    {'lockId': self._lock_id}, ConsistencyLevel.QUORUM)

    def _verify_lock(self, response):
        if len(response) == 0:
            return self.release().addCallback(lambda _: defer.fail(
                NoLockClaimsError(self._lock_table, self._lock_id)))

        if response[0]['claimId'] == self._claim_id:
            return defer.succeed(True)
        else:
            if self._log:
                self._log.msg('Got different claimId: {}'.format(response[0]['claimId']),
                              diff_claim_id=response[0]['claimId'], **self._log_kwargs)
            return self.release().addCallback(lambda _: defer.fail(
                BusyLockError(self._lock_table, self._lock_id)))

    def _write_lock(self):
        query = 'INSERT INTO {cf} ("lockId","claimId") VALUES (:lockId,:claimId) USING TTL {ttl};'
        return self._client.execute(query.format(cf=self._lock_table, ttl=self._ttl),
                                    {'lockId': self._lock_id, 'claimId': self._claim_id},
                                    ConsistencyLevel.QUORUM)

    def _write_lock_trans(self):
        query = ('INSERT INTO {cf} ("lockId","claimId") VALUES (:lockId,:claimId) '
                 'IF NOT EXISTS USING TTL {ttl};')
        return self._client.execute(query.format(cf=self._lock_table, ttl=self._ttl),
                                    {'lockId': self._lock_id, 'claimId': self._claim_id},
                                    ConsistencyLevel.QUORUM)

    @staticmethod
    def ensure_schema(client, table_name):
        """
        Create the table/columnfamily if it doesn't already exist.

        :param client: A Cassandra CQL client
        :type client: silverberg.client.CQLClient

        :param lock_table: A table/columnfamily table name for holding locks.
        :type lock_table: str
        """
        query = ''.join([
            'CREATE TABLE {cf} ',
            '("lockId" ascii, "claimId" uuid, PRIMARY KEY("lockId"));'])

        def errback(failure):
            failure.trap(InvalidRequestException)

        return client.execute(query.format(cf=table_name),
                              {}, ConsistencyLevel.QUORUM).addErrback(errback)

    @staticmethod
    def drop_schema(client, table_name):
        """
        Delete the table/columnfamily.

        :param client: A Cassandra CQL client
        :type client: silverberg.client.CQLClient

        :param lock_table: A table/columnfamily table name for holding locks.
        :type lock_table: str
        """
        query = 'DROP TABLE {cf}'
        return client.execute(query.format(cf=table_name),
                              {}, ConsistencyLevel.QUORUM)

    def release(self):
        """
        Release the lock.
        """
        if self._loop:
            self._loop.stop()
            self._loop = None

        query = 'DELETE FROM {cf} WHERE "lockId"=:lockId;'
        d = self._client.execute(query.format(cf=self._lock_table),
                                 {'lockId': self._lock_id}, ConsistencyLevel.QUORUM)

        def _log_release_time(result):
            if self._log and self._lock_acquired_seconds is not None:
                seconds = self._reactor.seconds() - self._lock_acquired_seconds
                self._log.msg('Released lock. Was held for {} seconds'.format(seconds),
                              lock_held_time=seconds, result=result, **self._log_kwargs)
            return result

        return d.addBoth(_log_release_time)

    def _hearbeat_lock(self, result):
        """
        Keep claiming the lock by inserting the claim row every
        `self._claim_interval` seconds
        """

        def write_lock():
            d = self._write_lock()
            d.addErrback(lambda f: (self._log and self._log.msg(
                'Error inserting claim', reason=f, **self._log_kwargs)))
            return d

        self._loop = task.LoopingCall(write_lock)
        self._loop.clock = self._reactor
        self._loop.start(self._claim_interval, False)
        return result

    def acquire(self):
        """
        Acquire the lock.

        If the lock can't be acquired immediately, retry a specified number of
        times, with a specified wait time.
        """
        retries = [0]
        self._acquire_start_seconds = self._reactor.seconds()

        def log_lock_acquired(result):
            self._lock_acquired_seconds = self._reactor.seconds()
            seconds = self._lock_acquired_seconds - self._acquire_start_seconds
            self._log.msg('Acquired lock in {} seconds'.format(seconds),
                          lock_acquire_time=seconds, **self._log_kwargs)
            return result

        def acquire_lock():
            d = self._write_lock()
            d.addCallback(self._read_lock)
            d.addCallback(self._verify_lock)
            #d.addCallback(self._hearbeat_lock)
            if self._log:
                d.addCallback(log_lock_acquired)
            d.addErrback(lock_not_acquired)
            return d

        def lock_not_acquired(failure):
            failure.trap(BusyLockError, NoLockClaimsError)
            retries[0] += 1
            if retries[0] <= self._max_retry:
                return task.deferLater(self._reactor, self._retry_wait, acquire_lock)
            else:
                return failure

        def log_lock_acquire_failure(failure):
            if self._log:
                seconds = self._reactor.seconds() - self._acquire_start_seconds
                self._log.msg(
                    'Could not acquire lock in {} seconds due to {}'.format(seconds, failure),
                    lock_acquire_fail_time=seconds, reason=failure, **self._log_kwargs)
            return failure

        return acquire_lock().addErrback(log_lock_acquire_failure)


class FileLock(Lock):
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

    class Log(object):
        def msg(self, *args, **kwargs):
            print(*args, **kwargs)

    lock = BasicLock(client, 'locks', 'schedule', max_retry=30,
                     retry_wait=random.uniform(0.5, 1.5), reactor=reactor,
                     log=log)
                     #log=type('Log', ('object',), dict(msg=print))())
    #lock = FileLock('schedule', max_retry=30, retry_wait=random.uniform(0.5, 1.5),
    #                clock=reactor)
    return lock.with_lock(_fetch_and_delete, now, size)


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
    log.startLogging(sys.stdout)
    clients = os.getenv('CASS_CLIENTS').split(',')
    print('cass clients', clients)
    hosts = [clientFromString(reactor, client) for client in clients]
    client = LoggingCQLClient(RoundRobinCassandraCluster(hosts, 'manitest'), log,
                              clock=reactor)
    mod = args[0]
    if mod == 'schema':
        return schema(clients)
    d = getattr(sys.modules[__name__], mod)(reactor, client, args[1:])
    d.addBoth(print_with_time, reactor, reactor.seconds(), mod)
    d.addBoth(lambda _: client.disconnect())
    return d

task.react(main, sys.argv[1:])

