"""
The Otter Controller:  Because otherwise your otters will make a mess of your
house.  Don't believe me?  There are videos on pet otters on youtube!

The Otter Controller manages a set of non-user visible state information for
each group, holds a lock on that state information, receives events from the
model object (group config change, scaling policy execution), and receives
events from the supervisor (job completed)

TODO:
 * Lock yak shaving
 * Eviction policy

Storage model for state information:
 * active list
    * Instance links
    * Created time
 * pending list
    * Job ID
 * last touched information for group
 * last touched information for policy
"""
from datetime import datetime
from decimal import Decimal, ROUND_UP
from functools import partial
import iso8601
import json

from twisted.internet import defer

from otter.log import audit
from otter.json_schema.group_schemas import MAX_ENTITIES
from otter.util.deferredutils import unwrap_first_error
from otter.util.timestamp import from_timestamp
from otter.worker.heat_worker import HeatWorker


class CannotExecutePolicyError(Exception):
    """
    Exception to be raised when the policy cannot be executed
    """
    def __init__(self, tenant_id, group_id, policy_id, why):
        super(CannotExecutePolicyError, self).__init__(
            "Cannot execute scaling policy {p} for group {g} for tenant {t}: {w}"
            .format(t=tenant_id, g=group_id, p=policy_id, w=why))


def pause_scaling_group(log, transaction_id, scaling_group):
    """
    Pauses the scaling group, causing all scaling policy executions to be
    rejected until unpaused.  This is an idempotent change, if it's already
    paused, this does not raise an error.

    :raises: :class:`NoSuchScalingGroup` if the scaling group does not exist.

    :return: None
    """
    raise NotImplementedError('Pause is not yet implemented')


def resume_scaling_group(log, transaction_id, scaling_group):
    """
    Resumes the scaling group, causing all scaling policy executions to be
    evaluated as normal again.  This is an idempotent change, if it's already
    paused, this does not raise an error.

    :raises: :class:`NoSuchScalingGroup` if the scaling group does not exist.

    :return: None
    """
    raise NotImplementedError('Resume is not yet implemented')


def _do_convergence_audit_log(_, log, delta, state):
    """
    Logs a convergence event to the audit log
    """
    audit_log = audit(log)

    if delta < 0:
        msg = "Deleting {0}".format(-delta)
        event_type = "convergence.scale_down"
    else:
        msg = "Starting {convergence_delta} new"
        event_type = "convergence.scale_up"

    msg += " servers to satisfy desired capacity"

    audit_log.msg(msg, event_type=event_type, convergence_delta=delta,
                  # setting policy_id/webhook_id to None is a hack to prevent
                  # them from making it into the audit log
                  policy_id=None, webhook_id=None,
                  **state.get_capacity())
    return state


def obey_config_change(log, transaction_id, config, scaling_group, state,
                       launch_config):
    """
    Given the config change, do servers need to be started or deleted

    Ignore all cooldowns.

    :param log: A twiggy bound log for logging
    :param str transaction_id: the transaction id
    :param dict config: the scaling group config
    :param dict launch_config: the scaling group launch config
    :param scaling_group: an IScalingGroup provider
    :param state: a :class:`otter.models.interface.GroupState` representing the
        state

    :return: a ``Deferred`` that fires with the updated (or not)
        :class:`otter.models.interface.GroupState` if successful
    """
    bound_log = log.bind(scaling_group_id=scaling_group.uuid)

    current = state.desired
    state.desired = constrain_desired(bound_log, current, config)

    d = scaling_group.view_launch_config()

    # shouldn't have to wait on convergence, but splitting up
    # convergence and executor is more work because it will have to
    # modify state again
    d.addCallback(partial(converge, bound_log, transaction_id,
                          state, config))
    d.addErrback(bound_log.err)
    d.addBoth(lambda _: state)
    return d


def maybe_execute_scaling_policy(
        log,
        transaction_id,
        scaling_group,
        state,
        policy_id, version=None):
    """
    Checks whether and how much a scaling policy can be executed.

    :param log: A twiggy bound log for logging
    :param str transaction_id: the transaction id
    :param scaling_group: an IScalingGroup provider
    :param state: a :class:`otter.models.interface.GroupState` representing the
        state
    :param policy_id: the policy id to execute
    :param version: the policy version to check before executing

    :return: a ``Deferred`` that fires with the updated
        :class:`otter.models.interface.GroupState` if successful

    :raises: :class:`NoSuchScalingGroupError` if this scaling group does not exist
    :raises: :class:`NoSuchPolicyError` if the policy id does not exist
    :raises: :class:`CannotExecutePolicyException` if the policy cannot be executed

    :raises: Some exception about why you don't want to execute the policy. This
        Exception should also have an audit log id
    """
    bound_log = log.bind(scaling_group_id=scaling_group.uuid, policy_id=policy_id)
    bound_log.msg("beginning to execute scaling policy")

    # make sure that the policy (and the group) exists before doing anything else
    deferred = scaling_group.get_policy(policy_id, version)

    def _do_get_configs(policy):
        deferred = defer.gatherResults([
            scaling_group.view_config(),
            scaling_group.view_launch_config()
        ])
        return deferred.addCallback(lambda results: results + [policy])

    deferred.addCallbacks(_do_get_configs, unwrap_first_error)

    def _do_maybe_execute(config_launch_policy):
        """
        state_config_policy should be returned by ``check_cooldowns``
        """
        config, launch, policy = config_launch_policy
        error_msg = "Cooldowns not met."

        if check_cooldowns(bound_log, state, config, policy, policy_id):
            current = state.desired
            desired = calculate_desired(bound_log, current, policy)
            state.desired = constrain_desired(bound_log, desired, config)

            execute_bound_log = bound_log.bind(prev_desired=current,
                                               curr_desired=state.desired)

            state.mark_executed(policy_id)

            # shouldn't have to wait on convergence, but splitting up
            # convergence and executor is more work because it will have to
            # modify state again
            d = converge(execute_bound_log, transaction_id, state, config,
                         launch)
            d.addBoth(lambda _: state)
            return d

        raise CannotExecutePolicyError(scaling_group.tenant_id,
                                       scaling_group.uuid, policy_id,
                                       error_msg)

    return deferred.addCallback(_do_maybe_execute)


_authenticate = None

def set_authenticate(authenticate):
    global _authenticate
    _authenticate = authenticate


def converge(log, transaction_id, state, config, launch_config):
    """
    Update/create stack
    """
    def _converge((auth_token, service_catalog)):
        # TODO: allow for either the auth token to be passed (for example when
        # taken directly from a request), or alternately get authentication
        # if this is an async converge independent of a request

        # TODO: pass the tenant's URL from the service catalog, or form it
        # from the tenant ID if the auth token is forwarded from the request
        worker = HeatWorker(state.tenant_id, state.group_id, launch_config,
                            state.desired, auth_token, log)
        if state.heat_stack:
            # TODO: if update is currently happening, raise
            # UpdateInProgressError, and have controller queue up another
            # converge after it's over, if another queued converge doesn't
            # exist yet. (1-length queue)
            d = worker.update_stack(state.heat_stack)
        else:
            d = worker.create_stack()

            def set_stack(link):
                state.heat_stack = link

            d.addCallback(set_stack)

        return d

    return _authenticate(state.tenant_id, log).addCallback(_converge)


def check_cooldowns(log, state, config, policy, policy_id):
    """
    Check the global cooldowns (when was the last time any policy was executed?)
    and the policy specific cooldown (when was the last time THIS policy was
    executed?)

    :param log: A twiggy bound log for logging
    :param dict state: the state dictionary
    :param dict config: the config dictionary
    :param dict policy: the policy dictionary
    :param str policy_id: the policy id that matches ``policy``

    :return: C{int}
    """
    this_now = datetime.now(iso8601.iso8601.UTC)

    timestamp_and_cooldowns = [
        (state.policy_touched.get(policy_id), policy['cooldown'], 'policy'),
        (state.group_touched, config['cooldown'], 'group'),
    ]

    for last_time, cooldown, cooldown_type in timestamp_and_cooldowns:
        if last_time is not None:
            delta = this_now - from_timestamp(last_time)
            if delta.total_seconds() < cooldown:
                log.bind(time_since_last_touched=delta.total_seconds(),
                         cooldown_type=cooldown_type,
                         cooldown_seconds=cooldown).msg("cooldown not reached")
                return False

    return True


def calculate_desired(log, current, policy):
    """
    Calculate the desired capacity, keeping in mind the minimum and maximum
    constraints.

    :param log: A twiggy bound log for logging
    :param dict state: the state dictionary
    :param dict config: the config dictionary
    :param dict policy: the policy dictionary

    :return: C{int} representing the desired change - can be 0
    """
    if "change" in policy:
        desired = current + policy['change']
    elif "changePercent" in policy:
        percentage = policy["changePercent"]
        change = int((current * (Decimal(percentage) / 100)).to_integral_value(ROUND_UP))
        desired = current + change
    elif "desiredCapacity" in policy:
        desired = policy["desiredCapacity"]
    else:
        raise AttributeError(
            "Policy doesn't have attributes 'change', 'changePercent', or "
            "'desiredCapacity: {0}".format(json.dumps(policy)))

    log.msg("calculating new desired capacity",
            previous_desired_capacity=current,
            unconstrained_desired_capacity=desired)

    return desired


def constrain_desired(log, desired, config):
    """
    Given the new desired capacity, constrains it with regards to min/max
    constraints.
    """
    # constrain the desired
    max_entities = config['maxEntities']
    if max_entities is None:
        max_entities = MAX_ENTITIES
    constrained_desired = max(min(desired, max_entities), config['minEntities'])

    log.msg("constraining new desired capacity",
            unconstrained_desired_capacity=desired,
            constrained_desired_capacity=constrained_desired,
            max_entities=max_entities, min_entities=config['minEntities'])

    return constrained_desired
