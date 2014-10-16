"""Tests for convergence."""

from effect import Effect, ConstantIntent
from effect.testing import StubIntent, resolve_stubs

from twisted.trial.unittest import SynchronousTestCase

from otter.test.utils import patch
from otter.convergence import (
    _converge_lb_state,
    get_all_server_details, get_scaling_group_servers,
    converge, Convergence, CreateServer, DeleteServer,
    RemoveFromLoadBalancer, ChangeLoadBalancerNode, AddToLoadBalancer,
    DesiredGroupState, NovaServer, Request, LBConfig, LBNode,
    ACTIVE, ERROR, BUILD,
    ServiceType, NodeCondition, NodeType)

from pyrsistent import pmap, pbag


class GetAllServerDetailsTests(SynchronousTestCase):
    """
    Tests for :func:`get_all_server_details`
    """

    def setUp(self):
        """Save default request and servers response."""
        self.req = ('GET', 'servers/detail?limit=10')
        self.servers = [{'id': i} for i in range(9)]

    def _request(self, requests):
        def request(method, url):
            response = requests.get((method, url))
            if response is None:
                raise KeyError("{} not in {}".format((method, url), requests.keys()))
            return Effect(StubIntent(ConstantIntent(response)))
        return request

    def test_get_all_less_limit(self):
        """
        `get_all_server_details` will not fetch again if first get returns results
        with size < limit
        """
        request = self._request({self.req: {'servers': self.servers}})
        eff = get_all_server_details(request, limit=10)
        self.assertEqual(resolve_stubs(eff), self.servers)

    def test_get_all_above_limit(self):
        """
        `get_all_server_details` will fetch again until batch returned has size < limit
        """
        servers = [{'id': i} for i in range(19)]
        req2 = ('GET', 'servers/detail?limit=10&marker=9')
        request = self._request({self.req: {'servers': servers[:10]},
                                 req2: {'servers': servers[10:]}})
        eff = get_all_server_details(request, limit=10)
        self.assertEqual(resolve_stubs(eff), servers)


class GetScalingGroupServersTests(SynchronousTestCase):
    """
    Tests for :func:`get_scaling_group_servers`
    """

    def setUp(self):
        """
        Mock and setup :func:`get_all_server_details`
        """
        self.mock_gasd = patch(self, 'otter.convergence.get_all_server_details')
        self.servers = []

        def gasd(*args, **kwargs):
            if args == ('r',) and kwargs == {}:
                return Effect(StubIntent(ConstantIntent(self.servers)))

        # Setup function to return value only on expected args to avoid asserting
        # its called every time
        self.mock_gasd.side_effect = gasd

    def test_filters_no_metadata(self):
        """
        Does not include servers which do not have metadata in it
        """
        self.servers = [{'id': i} for i in range(10)]
        d = get_scaling_group_servers('r')
        self.assertEqual(resolve_stubs(d), {})

    def test_filters_no_as_metadata(self):
        """
        Does not include servers which have metadata but does not have AS info in it
        """
        self.servers = [{'id': i, 'metadata': {}} for i in range(10)]
        d = get_scaling_group_servers('r')
        self.assertEqual(resolve_stubs(d), {})

    def test_returns_as_servers(self):
        """
        Returns servers with AS metadata in it grouped by scaling group ID
        """
        as_servers = (
            [{'metadata': {'rax:auto_scaling_group_id': 'a'}, 'id': i} for i in range(5)] +
            [{'metadata': {'rax:auto_scaling_group_id': 'b'}, 'id': i} for i in range(5, 8)] +
            [{'metadata': {'rax:auto_scaling_group_id': 'a'}, 'id': 10}])
        self.servers = as_servers + [{'metadata': 'junk'}] * 3
        d = get_scaling_group_servers('r')
        self.assertEqual(
            resolve_stubs(d),
            {'a': as_servers[:5] + [as_servers[-1]], 'b': as_servers[5:8]})

    def test_filters_on_user_criteria(self):
        """
        Considers user provided filter if provided
        """
        as_servers = (
            [{'metadata': {'rax:auto_scaling_group_id': 'a'}, 'id': i} for i in range(5)] +
            [{'metadata': {'rax:auto_scaling_group_id': 'b'}, 'id': i} for i in range(5, 8)])
        self.servers = as_servers + [{'metadata': 'junk'}] * 3
        d = get_scaling_group_servers('r',
                                      server_predicate=lambda s: s['id'] % 3 == 0)
        self.assertEqual(
            resolve_stubs(d),
            {'a': [as_servers[0], as_servers[3]], 'b': [as_servers[6]]})


class ObjectStorageTests(SynchronousTestCase):
    """
    Tests for objects that store data such as :class:`LBConfig`
    """
    def test_lbconfig_default_weight_condition_and_type(self):
        """
        The required values for a LBConfig are just the lb_id and port.  The
        other attributes have default values.
        """
        lb = LBConfig(lb_id=1, port=80)
        self.assertEqual(lb.weight, 1)
        self.assertEqual(lb.condition, NodeCondition.ENABLED)
        self.assertEqual(lb.type, NodeType.PRIMARY)


class ConvergeLBStateTests(SynchronousTestCase):
    """
    Tests for :func:`_converge_lb_state`
    """
    def test_add_to_lb(self):
        """
        If a desired LB config is not in the set of current configs,
        `converge_lb_state` returns a :class:`AddToLoadBalancer` object
        """
        result = _converge_lb_state(desired_lb_state=[LBConfig(lb_id=5, port=80)],
                                    current_lb_state=[],
                                    ip_address='1.1.1.1')
        self.assertEqual(
            list(result),
            [AddToLoadBalancer(loadbalancer_id=5, address='1.1.1.1', port=80,
                               condition=NodeCondition.ENABLED,
                               type=NodeType.PRIMARY, weight=1)])

    def test_change_lb_node(self):
        """
        If a desired LB mapping is in the set of current configs,
        but the configuration is wrong, `converge_lb_state` returns a
        :class:`ChangeLoadBalancerNode` object
        """
        desired = [LBConfig(lb_id=5, port=80)]
        current = [LBNode(node_id=123, address='1.1.1.1',
                          config=LBConfig(lb_id=5, port=80, weight=5))]

        result = _converge_lb_state(desired_lb_state=desired,
                                    current_lb_state=current,
                                    ip_address='1.1.1.1')
        self.assertEqual(
            list(result),
            [ChangeLoadBalancerNode(loadbalancer_id=5, node_id=123, weight=1,
                                    condition=NodeCondition.ENABLED,
                                    type=NodeType.PRIMARY)])

    def test_remove_lb_node(self):
        """
        If a current lb config is not in the desired set of lb configs,
        `converge_lb_state` returns a :class:`RemoveFromLoadBalancer` object
        """
        current = [LBNode(node_id=123, address='1.1.1.1',
                          config=LBConfig(lb_id=5, port=80, weight=5))]

        result = _converge_lb_state(desired_lb_state=[],
                                    current_lb_state=current,
                                    ip_address='1.1.1.1')
        self.assertEqual(
            list(result),
            [RemoveFromLoadBalancer(loadbalancer_id=5, node_id=123)])

    def test_do_nothing(self):
        """
        If the desired lb state matches the current lb state,
        `converge_lb_state` returns nothing
        """
        desired = [LBConfig(lb_id=5, port=80)]
        current = [LBNode(node_id=123, address='1.1.1.1',
                          config=LBConfig(lb_id=5, port=80))]

        result = _converge_lb_state(desired_lb_state=desired,
                                    current_lb_state=current,
                                    ip_address='1.1.1.1')
        self.assertEqual(list(result), [])

    def test_all_changes(self):
        """
        Remove, change, and add a node to a load balancer all together
        """
        desired = [LBConfig(lb_id=5, port=80),
                   LBConfig(lb_id=6, port=80, weight=2)]
        current = [LBNode(node_id=123, address='1.1.1.1',
                          config=LBConfig(lb_id=5, port=8080)),
                   LBNode(node_id=234, address='1.1.1.1',
                          config=LBConfig(lb_id=6, port=80))]

        result = _converge_lb_state(desired_lb_state=desired,
                                    current_lb_state=current,
                                    ip_address='1.1.1.1')
        self.assertEqual(set(result), set([
            AddToLoadBalancer(loadbalancer_id=5, address='1.1.1.1', port=80,
                              condition=NodeCondition.ENABLED,
                              type=NodeType.PRIMARY, weight=1),
            ChangeLoadBalancerNode(loadbalancer_id=6, node_id=234, weight=2,
                                   condition=NodeCondition.ENABLED,
                                   type=NodeType.PRIMARY),
            RemoveFromLoadBalancer(loadbalancer_id=5, node_id=123)
        ]))


def server(id, state, created=0, **kwargs):
    """Convenience for creating a :obj:`NovaServer`."""
    return NovaServer(id=id, state=state, created=created, **kwargs)


class ConvergeTests(SynchronousTestCase):
    """Tests for :func:`converge`."""

    def test_converge_give_me_a_server(self):
        """
        A server is added if there are not enough servers to meet
        the desired capacity.
        """
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=1),
                [],
                {},
                0),
            Convergence(
                steps=pbag([CreateServer(launch_config=pmap())])))

    def test_converge_give_me_multiple_servers(self):
        """
        Multiple servers are added at a time if there are not enough servers to
        meet the desired capacity.
        """
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=2),
                [],
                {},
                0),
            Convergence(
                steps=pbag([
                    CreateServer(launch_config=pmap()),
                    CreateServer(launch_config=pmap())])))

    def test_count_building_as_meeting_capacity(self):
        """
        No servers are created if there are building servers that sum with
        active servers to meet capacity.
        """
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=1),
                [server('abc', BUILD)],
                {},
                0),
            Convergence(steps=pbag([])))

    def test_delete_nodes_in_error_state(self):
        """
        If a server we created enters error state, it will be deleted if
        necessary, and replaced.
        """
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=1),
                [server('abc', ERROR)],
                {},
                0),
            Convergence(
                steps=pbag([
                    DeleteServer(server_id='abc'),
                    CreateServer(launch_config=pmap()),
                ])))

    def test_delete_error_state_servers_with_lb_nodes(self):
        """
        If a server we created enters error state and it is attached to one
        or more load balancers, it will be removed from its load balancers
        as well as get deleted.
        """
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=1),
                [server('abc', ERROR, servicenet_address='1.1.1.1')],
                [LBNode(address='1.1.1.1', node_id=3,
                        config=LBConfig(lb_id=5, port=80)),
                 LBNode(address='1.1.1.1', node_id=5,
                        config=LBConfig(lb_id=5, port=8080))],
                0),
            Convergence(
                steps=pbag([
                    DeleteServer(server_id='abc'),
                    RemoveFromLoadBalancer(loadbalancer_id=5, node_id=3),
                    RemoveFromLoadBalancer(loadbalancer_id=5, node_id=5),
                    CreateServer(launch_config=pmap()),
                ])))

    def test_scale_down(self):
        """If we have more servers than desired, we delete the oldest."""
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=1),
                [server('abc', ACTIVE, created=0),
                 server('def', ACTIVE, created=1)],
                {},
                0),
            Convergence(steps=pbag([DeleteServer(server_id='abc')])))

    def test_scale_down_with_lb_nodes(self):
        """
        When scaling down, if there are any servers to be deleted that are
        attached to existing load balancers, they will also be also removed
        from said load balancers
        """
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=0),
                [server('abc', ACTIVE, servicenet_address='1.1.1.1', created=0)],
                [LBNode(address='1.1.1.1', node_id=3,
                        config=LBConfig(lb_id=5, port=80))],
                0),
            Convergence(steps=pbag([
                DeleteServer(server_id='abc'),
                RemoveFromLoadBalancer(loadbalancer_id=5, node_id=3)
            ])))

    def test_scale_down_building_first(self):
        """
        When scaling down, first we delete building servers, in preference
        to older server.
        """
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=2),
                [server('abc', ACTIVE, created=0),
                 server('def', BUILD, created=1),
                 server('ghi', ACTIVE, created=2)],
                {},
                0),
            Convergence(
                steps=pbag([DeleteServer(server_id='def')])))

    def test_timeout_building(self):
        """
        Servers that have been building for too long will be deleted and
        replaced.
        """
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=2),
                [server('slowpoke', BUILD, created=0),
                 server('ok', ACTIVE, created=0)],
                {},
                3600),
            Convergence(
                steps=pbag([
                    DeleteServer(server_id='slowpoke'),
                    CreateServer(launch_config=pmap())])))

    def test_timeout_replace_only_when_necessary(self):
        """
        If a server is timing out *and* we're over capacity, it will be
        deleted without replacement.
        """
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=2),
                [server('slowpoke', BUILD, created=0),
                 server('old-ok', ACTIVE, created=0),
                 server('new-ok', ACTIVE, created=3600)],
                {},
                3600),
            Convergence(steps=pbag([DeleteServer(server_id='slowpoke')])))

    def test_converge_active_servers_ignores_servers_to_be_deleted(self):
        """
        Only servers in active that are not being deleted will have their
        load balancers converged.
        """
        desired_lbs = [LBConfig(lb_id=5, port=80)]
        self.assertEqual(
            converge(
                DesiredGroupState(launch_config={}, desired=1),
                [server('abc', ACTIVE, servicenet_address='1.1.1.1', created=0,
                        desired_lbs=desired_lbs),
                 server('bcd', ACTIVE, servicenet_address='2.2.2.2', created=1,
                        desired_lbs=desired_lbs)],
                [],
                0),
            Convergence(steps=pbag([
                DeleteServer(server_id='abc'),
                AddToLoadBalancer(loadbalancer_id=5, address='2.2.2.2', port=80,
                                  condition=NodeCondition.ENABLED, weight=1,
                                  type=NodeType.PRIMARY)
            ])))


# time out (delete) building servers
# load balancers!


class RequestConversionTests(SynchronousTestCase):
    """
    Tests for converting ISteps to :obj:`Request`s.
    """

    def test_create_server(self):
        """
        :obj:`CreateServer.as_request` produces a request for creating a server.
        """
        create = CreateServer(launch_config=pmap({'name': 'myserver', 'flavorRef': '1'}))
        self.assertEqual(
            create.as_request(),
            Request(
                service=ServiceType.CLOUD_SERVERS,
                method='POST',
                path='servers',
                data=pmap({'name': 'myserver', 'flavorRef': '1'})))

    def test_delete_server(self):
        """
        :obj:`DeleteServer.as_request` produces a request for deleting a server.
        """
        delete = DeleteServer(server_id='abc123')
        self.assertEqual(
            delete.as_request(),
            Request(
                service=ServiceType.CLOUD_SERVERS,
                method='DELETE',
                path='servers/abc123'))

    def test_remove_from_load_balancer(self):
        """
        :obj:`RemoveFromLoadBalancer.as_request` produces a request for
        removing a node from a load balancer.
        """
        lbremove = RemoveFromLoadBalancer(
            loadbalancer_id='abc123',
            node_id='node1')
        self.assertEqual(
            lbremove.as_request(),
            Request(
                service=ServiceType.CLOUD_LOAD_BALANCERS,
                method='DELETE',
                path='loadbalancers/abc123/node1'))

    def test_change_load_balancer_node(self):
        """
        :obj:`ChangeLoadBalancerNode.as_request` produces a request for
        modifying a load balancer node.
        """
        changenode = ChangeLoadBalancerNode(
            loadbalancer_id='abc123',
            node_id='node1',
            condition='DRAINING',
            weight=50,
            type="PRIMARY")
        self.assertEqual(
            changenode.as_request(),
            Request(
                service=ServiceType.CLOUD_LOAD_BALANCERS,
                method='PUT',
                path='loadbalancers/abc123/nodes/node1',
                data={'condition': 'DRAINING',
                      'weight': 50}))
