"""
The OtterClock.  Because, while a broken clock is right twice a day, an OtterClock
is right all the time and is probably what caused your regular clock to get broken
in the first place.
"""

from datetime import datetime
from functools import partial

from twisted.application.service import MultiService
from twisted.internet import defer

from otter.controller import (
    CannotExecutePolicyError, maybe_execute_scaling_policy, modify_and_trigger)
from otter.log import log as otter_log
from otter.log.bound import bound_log_kwargs
from otter.models.interface import (
    NoSuchPolicyError, NoSuchScalingGroupError, next_cron_occurrence)
from otter.util.deferredutils import ignore_and_log
from otter.util.hashkey import generate_transaction_id


class SchedulerService(MultiService, object):
    """
    Service to trigger scheduled events
    """

    def __init__(self, dispatcher, batchsize, store, partitioner,
                 threshold=60, clock=None):
        """
        Initialize the scheduler service

        :param dispatcher: Effect dispatcher
        :param int batchsize: number of events to fetch on each iteration
        :param store: cassandra store
        :param partitioner_factory: Callable of (log, callback) ->
            :obj:`Partitioner`
        """
        MultiService.__init__(self)
        self.store = store
        self.threshold = threshold
        self.log = otter_log.bind(system='otter.scheduler')
        self.partitioner = partitioner
        self.dispatcher = dispatcher
        ts = TimerService(interval, self._check_events, batchsize)
        ts.clock = clock
        ts.setServiceParent(self)

    def health_check(self):
        """
        Check if scheduler service is healthy by comparing oldest event to
        current time. If the oldest event is older than the threshold, then
        we're considered unhealthy.

        :return: Deferred that fires with tuple (Bool, `dict` of extra debug
        info)
        """
        if not self.running:
            return False, {'reason': 'Not running'}

        if not self.partitioner.is_acquired():
            return False, {"reason": "partitioner not acquired"}

        def check_older_events(events):
            now = datetime.utcnow()
            old_events = []
            for event in events:
                if event and (now - event['trigger']).total_seconds() > self.threshold:
                    event['version'] = str(event['version'])
                    event['trigger'] = str(event['trigger'])
                    old_events.append(event)
            return (not bool(old_events), {"old_events": old_events})

        buckets = self.partitioner.get_current_buckets()
        d = defer.gatherResults(
            [self.store.get_oldest_event(bucket) for bucket in buckets],
            consumeErrors=True)
        return d.addCallback(check_older_events)

    def _check_events(self, batchsize):
        """
        Check for events occurring now and earlier
        """
        if not self.partitioner.is_acquired():
            return

        utcnow = datetime.utcnow()
        log = self.log.bind(scheduler_run_id=generate_transaction_id(),
                            utcnow=utcnow)
        buckets = self.partitioner.get_current_buckets()
        return defer.gatherResults(
            [check_events_in_bucket(
                log, self.dispatcher, self.store, bucket, utcnow, batchsize)
             for bucket in buckets])


def check_events_in_bucket(log, dispatcher, store, bucket, now, batchsize):
    """
    Retrieves events in the given bucket that occur before or at now,
    in batches of batchsize, for processing

    :param log: A bound log for logging
    :param dispatcher: Effect dispatcher
    :param store: `IScalingGroupCollection` provider
    :param bucket: Bucket to check events in
    :param now: Time before which events are checked
    :param batchsize: Number of events to check at a time

    :return: a deferred that fires with None
    """

    log = log.bind(bucket=bucket)

    def check_for_more(num_events):
        if num_events == batchsize:
            return _do_check()

    def _do_check():
        d = store.fetch_and_delete(bucket, now, batchsize)
        d.addCallback(process_events, dispatcher, store, log)
        d.addCallback(check_for_more)
        d.addErrback(log.err)
        return d

    return _do_check()


def process_events(events, dispatcher, store, log):
    """
    Executes all the events and adds the next occurrence of each event
    to the buckets

    :param events: list of event dict to process
    :param dispatcher: Effect dispatcher
    :param store: `IScalingGroupCollection` provider
    :param log: A bound log for logging

    :return: a `Deferred` that fires with number of events processed
    """
    if not events:
        return 0

    log.msg('Processing {num_events} events', num_events=len(events))

    deleted_policy_ids = set()

    deferreds = [
        execute_event(dispatcher, store, log, event, deleted_policy_ids)
        for event in events
    ]
    d = defer.gatherResults(deferreds, consumeErrors=True)
    d.addCallback(lambda _: add_cron_events(store, log, events, deleted_policy_ids))
    return d.addCallback(lambda _: len(events))


def add_cron_events(store, log, events, deleted_policy_ids):
    """
    Update events with cron entry with next trigger time.

    :param store: `IScalingGroupCollection` provider
    :param log: A bound log for logging
    :param events: list of event dict whose next event has to be added
    :param deleted_policy_ids: set of policy ids that have been deleted. Events
                               corresponding to these policy ids will not be added

    :return: `Deferred` that fires will result of adding cron events or None if no
             events have to be added
    """
    if not events:
        return

    new_cron_events = []
    for event in events:
        if event['cron'] and event['policyId'] not in deleted_policy_ids:
            event['trigger'] = next_cron_occurrence(event['cron'])
            new_cron_events.append(event)

    if new_cron_events:
        log.msg('Adding {new_cron_events} cron events', new_cron_events=len(new_cron_events))
        return store.add_cron_events(new_cron_events)


def execute_event(dispatcher, store, log, event, deleted_policy_ids):
    """
    Execute a single event

    :param dispatcher: Effect dispatcher
    :param store: `IScalingGroupCollection` provider
    :param log: A bound log for logging
    :param event: event dict to execute
    :param deleted_policy_ids: Set of policy ids that are deleted. Policy id
        will be added to this if its scaling group or policy has been deleted
    :return: a deferred with None. Any error occurred during execution is
        logged
    """
    tenant_id = event['tenantId']
    group_id = event['groupId']
    policy_id = event['policyId']
    log = log.bind(tenant_id=tenant_id, scaling_group_id=group_id,
                   policy_id=policy_id,
                   scheduled_time=event["trigger"].isoformat() + "Z")
    log.msg('sch-exec-pol', cloud_feed=True)
    group = store.get_scaling_group(log, tenant_id, group_id)
    d = modify_and_trigger(
        dispatcher,
        group,
        bound_log_kwargs(log),
        partial(maybe_execute_scaling_policy,
                log, generate_transaction_id(),
                policy_id=policy_id, version=event['version']),
        modify_state_reason='scheduler.execute_event')
    d.addErrback(ignore_and_log, CannotExecutePolicyError,
                 log, "sch-cannot-exec", cloud_feed=True)

    def collect_deleted_policy(failure):
        failure.trap(NoSuchScalingGroupError, NoSuchPolicyError)
        deleted_policy_ids.add(policy_id)

    d.addErrback(collect_deleted_policy)
    d.addErrback(log.err, "sch-exec-pol-err", cloud_feed=True)
    return d
