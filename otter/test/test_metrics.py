"""
Tests for `metrics.py`
"""

import mock
import json
from io import StringIO
import operator

from effect import Effect, ConstantIntent
from effect.testing import StubIntent

from pyrsistent import freeze

from toolz.dicttoolz import merge

from twisted.trial.unittest import SynchronousTestCase
from twisted.internet.defer import succeed, fail
from twisted.internet.task import Clock
from twisted.internet.base import ReactorBase

from otter.constants import get_service_mapping
from otter.metrics import (
    get_scaling_groups, get_tenant_metrics, get_all_metrics, GroupMetrics,
    add_to_cloud_metrics, collect_metrics, MetricsService, makeService, Options,
    get_all_metrics_effects, metrics_log)
from otter.test.test_auth import identity_config
from otter.auth import IAuthenticator
from otter.test.utils import (
    patch, StubTreq2, matches, IsCallable, CheckFailure, mock_log, resolve_retry_stubs,
    Provides)
from otter.util.http import headers
from otter.log import BoundLog

from testtools.matchers import IsInstance

from silverberg.client import CQLClient


class GetScalingGroupsTests(SynchronousTestCase):
    """
    Tests for :func:`get_scaling_groups`
    """

    def setUp(self):
        """
        Mock
        """
        self.client = mock.Mock(spec=CQLClient)
        self.exec_args = {}

        def _exec(query, params, c):
            return succeed(self.exec_args[freeze((query, params))])

        self.client.execute.side_effect = _exec
        self.select = ('SELECT "groupId","tenantId",active,created_at,desired,pending '
                       'FROM scaling_group ')

    def _add_exec_args(self, query, params, ret):
        self.exec_args[freeze((query, params))] = ret

    def test_all_groups_less_than_batch(self):
        """
        Works when number of all groups of all tenants < batch size
        """
        groups = [{'tenantId': i, 'groupId': j, 'desired': 3, 'created_at': 'c'}
                  for i in range(2) for j in range(2)]
        self._add_exec_args(self.select + ' LIMIT :limit;', {'limit': 5}, groups)
        d = get_scaling_groups(self.client, batch_size=5)
        self.assertEqual(list(self.successResultOf(d)), groups)

    def test_filters_no_created_or_desired(self):
        """
        Does not include groups that do not have created_at or desired
        """
        groups = [{'tenantId': 1, 'groupId': 2, 'desired': 3, 'created_at': None},
                  {'tenantId': 1, 'groupId': 3, 'desired': None, 'created_at': 'c'},
                  {'tenantId': 1, 'groupId': 4, 'desired': None, 'created_at': None},
                  {'tenantId': 1, 'groupId': 5, 'desired': 3, 'created_at': 'c'}]
        self._add_exec_args(self.select + ' LIMIT :limit;', {'limit': 5}, groups)
        d = get_scaling_groups(self.client, batch_size=5)
        self.assertEqual(list(self.successResultOf(d)), groups[-1:])

    def test_filters_on_group_pred_arg(self):
        """
        If group_pred arg is given then returns groups for which group_pred returns True
        """
        groups = [{'tenantId': 1, 'groupId': 2, 'desired': 3, 'created_at': 'c'},
                  {'tenantId': 1, 'groupId': 3, 'desired': 2, 'created_at': 'c'},
                  {'tenantId': 1, 'groupId': 4, 'desired': 6, 'created_at': 'c'},
                  {'tenantId': 1, 'groupId': 5, 'desired': 4, 'created_at': 'c'}]
        self._add_exec_args(self.select + ' LIMIT :limit;', {'limit': 5}, groups)
        d = get_scaling_groups(self.client, batch_size=5,
                               group_pred=lambda g: g['desired'] % 3 == 0)
        self.assertEqual(list(self.successResultOf(d)), [groups[0], groups[2]])

    def test_gets_props(self):
        """
        If props arg is given then returns groups with that property in it
        """
        groups = [{'tenantId': 1, 'groupId': 2, 'desired': 3, 'created_at': 'c', 'launch': 'l'},
                  {'tenantId': 1, 'groupId': 3, 'desired': 2, 'created_at': 'c', 'launch': 'b'}]
        self._add_exec_args(
            ('SELECT "groupId","tenantId",active,created_at,desired,launch,pending '
             'FROM scaling_group  LIMIT :limit;'),
            {'limit': 5}, groups)
        d = get_scaling_groups(self.client, props=['launch'], batch_size=5)
        self.assertEqual(list(self.successResultOf(d)), groups)

    def test_last_tenant_has_less_groups(self):
        """
        Fetches initial batch, then gets all groups of last tenant in that batch
        and stops when there are no more tenants
        """
        groups = [{'tenantId': 1, 'groupId': i, 'desired': 3, 'created_at': 'c'}
                  for i in range(7)]
        self._add_exec_args(self.select + ' LIMIT :limit;', {'limit': 5}, groups[:5])
        self._add_exec_args(
            self.select + 'WHERE "tenantId"=:tenantId AND "groupId">:groupId LIMIT :limit;',
            {'limit': 5, 'tenantId': 1, 'groupId': 4}, groups[5:])
        self._add_exec_args(
            self.select + 'WHERE token("tenantId") > token(:tenantId) LIMIT :limit;',
            {'limit': 5, 'tenantId': 1}, [])
        d = get_scaling_groups(self.client, batch_size=5)
        self.assertEqual(list(self.successResultOf(d)), groups)

    def test_many_tenants_having_more_than_batch_groups(self):
        """
        Gets all groups when there are many tenants each of them having groups > batch size
        """
        groups1 = [{'tenantId': 1, 'groupId': i, 'desired': 3, 'created_at': 'c'}
                   for i in range(7)]
        groups2 = [{'tenantId': 2, 'groupId': i, 'desired': 4, 'created_at': 'c'}
                   for i in range(9)]
        self._add_exec_args(self.select + ' LIMIT :limit;', {'limit': 5}, groups1[:5])
        self._add_exec_args(
            self.select + 'WHERE "tenantId"=:tenantId AND "groupId">:groupId LIMIT :limit;',
            {'limit': 5, 'tenantId': 1, 'groupId': 4}, groups1[5:])
        self._add_exec_args(
            self.select + 'WHERE token("tenantId") > token(:tenantId) LIMIT :limit;',
            {'limit': 5, 'tenantId': 1}, groups2[:5])
        self._add_exec_args(
            self.select + 'WHERE "tenantId"=:tenantId AND "groupId">:groupId LIMIT :limit;',
            {'limit': 5, 'tenantId': 2, 'groupId': 4}, groups2[5:])
        self._add_exec_args(
            self.select + 'WHERE token("tenantId") > token(:tenantId) LIMIT :limit;',
            {'limit': 5, 'tenantId': 2}, [])
        d = get_scaling_groups(self.client, batch_size=5)
        self.assertEqual(list(self.successResultOf(d)), groups1 + groups2)


class GetTenantMetricsTests(SynchronousTestCase):
    """Tests for :func:`get_tenant_metrics`"""

    def test_get_tenant_metrics(self):
        """Extracts metrics from the servers."""
        servers = {'g1': [{'status': 'ACTIVE'}] * 3 + [{'status': 'BUILD'}] * 2}
        groups = [{'groupId': 'g1', 'desired': 3}, {'groupId': 'g2', 'desired': 4}]
        self.assertEqual(
            get_tenant_metrics('t', groups, servers),
            [GroupMetrics('t', 'g1', 3, 3, 2), GroupMetrics('t', 'g2', 4, 0, 0)])


class GetAllMetricsEffectsTests(SynchronousTestCase):
    """Tests for :func:`get_all_metrics_effects`"""

    def _server(self, group, state):
        return {'status': state,
                'metadata': {'rax:auto_scaling_group_id': group}}

    def test_get_all_metrics(self):
        """
        Metrics are returned based on the requests done to get server info.
        """
        # Maybe this could use a parameterized "get_scaling_group_servers" call
        # to avoid needing to stub the nova responses, but it seems okay.
        servers_t1 = {
            'servers': (
                [self._server('g1', 'ACTIVE')] * 3
                + [self._server('g1', 'BUILD')] * 2
                + [self._server('g2', 'ACTIVE')])}

        servers_t2 = {
            'servers': [self._server('g4', 'ACTIVE'),
                        self._server('g4', 'BUILD')]}

        groups = [{'tenantId': 't1', 'groupId': 'g1', 'desired': 3},
                  {'tenantId': 't1', 'groupId': 'g2', 'desired': 4},
                  {'tenantId': 't2', 'groupId': 'g4', 'desired': 2}]

        tenant_servers = {'t1': servers_t1, 't2': servers_t2}

        def get_bound_request_func(tenant_id):
            def request_func(service_type, method, url, headers=None, data=None):
                return Effect(StubIntent(ConstantIntent(tenant_servers[tenant_id])))
            return request_func
        effs = get_all_metrics_effects(groups, get_bound_request_func)

        # All of the HTTP requests are wrapped in retries, so unwrap them
        results = map(resolve_retry_stubs, effs)

        self.assertEqual(
            set(reduce(operator.add, results)),
            set([GroupMetrics('t1', 'g1', desired=3, actual=3, pending=2),
                 GroupMetrics('t1', 'g2', desired=4, actual=1, pending=0),
                 GroupMetrics('t2', 'g4', desired=2, actual=1, pending=1)]))


class GnarlyGetMetricsTests(SynchronousTestCase):
    """
    Tests for :func:`get_all_metrics`.

    These tests aren't very nice -- they should eventually disappear, once more
    code is converted to using effects, and we don't need as much mocking.
    """

    def setUp(self):
        """Mock get_scaling_group_servers and get_request_func."""
        self.tenant_servers = {}
        # This is pretty nasty.

        # get_request_func is being mocked to just return the tenant id,
        # instead of a function. Nothing will call it, so it works.
        # Then, get_scaling_group_servers is being mocked to expect the tenant
        # ID instead of a request function, to use it to look up the server
        # data to return.
        self.mock_get_request_func = patch(
            self, 'otter.metrics.get_request_func',
            side_effect=lambda a, tenant_id, *args, **kwargs: tenant_id)
        self.mock_gsgs = patch(
            self, 'otter.metrics.get_scaling_group_servers',
            side_effect=lambda rf, server_predicate: (
                Effect(ConstantIntent(self.tenant_servers[rf]))))

    def test_get_all_metrics(self):
        """
        Gets group's metrics
        """
        servers_t1 = {'g1': [{'status': 'ACTIVE'}] * 3 + [{'status': 'BUILD'}] * 2,
                      'g2': [{'status': 'ACTIVE'}]}
        servers_t2 = {'g4': [{'status': 'ACTIVE'}, {'status': 'BUILD'}]}
        groups = [{'tenantId': 't1', 'groupId': 'g1', 'desired': 3},
                  {'tenantId': 't1', 'groupId': 'g2', 'desired': 4},
                  {'tenantId': 't2', 'groupId': 'g4', 'desired': 2}]

        self.tenant_servers['t1'] = servers_t1
        self.tenant_servers['t2'] = servers_t2

        authenticator = mock.Mock()

        d = get_all_metrics(
            groups, authenticator,
            {'cloudServersOpenStack': 'cloudServersOpenStack'},
            'r', clock='c')

        self.assertEqual(
            set(self.successResultOf(d)),
            set([GroupMetrics('t1', 'g1', 3, 3, 2), GroupMetrics('t1', 'g2', 4, 1, 0),
                 GroupMetrics('t2', 'g4', 2, 1, 1)]))
        self.mock_gsgs.assert_any_call('t1', server_predicate=IsCallable())
        self.mock_gsgs.assert_any_call('t2', server_predicate=IsCallable())

        self.mock_get_request_func.assert_any_call(
            authenticator, 't1', metrics_log,
            get_service_mapping({'cloudServersOpenStack': 'cloudServersOpenStack'}.get), 'r')
        self.mock_get_request_func.assert_any_call(
            authenticator, 't2', metrics_log,
            get_service_mapping({'cloudServersOpenStack': 'cloudServersOpenStack'}.get), 'r')


class AddToCloudMetricsTests(SynchronousTestCase):
    """
    Tests for :func:`add_to_cloud_metrics`
    """

    def setUp(self):
        """
        Setup treq
        """
        self.resp = {
            'access': {
                'token': {'id': 'token'},
                'serviceCatalog': [
                    {'endpoints': [{"region": "IAD", "publicURL": "url"}],
                     "name": "cloudMetricsIngest"}
                ]
            }
        }
        self.au = patch(self, 'otter.metrics.authenticate_user',
                        return_value=succeed(self.resp))

    @mock.patch('otter.metrics.time')
    def test_added(self, mock_time):
        """
        total desired, pending and actual are added to cloud metrics
        """
        td = 10
        ta = 20
        tp = 3
        mock_time.time.return_value = 100
        m = {'collectionTime': 100000, 'ttlInSeconds': 5 * 24 * 60 * 60}
        md = merge(m, {'metricValue': td, 'metricName': 'ord.desired'})
        ma = merge(m, {'metricValue': ta, 'metricName': 'ord.actual'})
        mp = merge(m, {'metricValue': tp, 'metricName': 'ord.pending'})
        req = ('POST', 'url/ingest', {'headers': headers('token'),
                                      'data': json.dumps([md, ma, mp])})
        treq = StubTreq2([(req, (200, ''))])
        conf = {'username': 'a', 'password': 'p', 'service': 'cloudMetricsIngest',
                'region': 'IAD', 'ttl': m['ttlInSeconds']}

        d = add_to_cloud_metrics(conf, 'idurl', 'ord', td, ta, tp, _treq=treq)

        self.assertIsNone(self.successResultOf(d))
        self.au.assert_called_once_with('idurl', 'a', 'p', log=matches(IsInstance(BoundLog)))


class CollectMetricsTests(SynchronousTestCase):
    """
    Tests for :func:`collect_metrics`
    """

    def setUp(self):
        """
        mock dependent functions
        """
        self.connect_cass_servers = patch(self, 'otter.metrics.connect_cass_servers')
        self.client = mock.Mock(spec=['disconnect'])
        self.client.disconnect.return_value = succeed(None)
        self.connect_cass_servers.return_value = self.client

        self.groups = mock.Mock()
        self.get_scaling_groups = patch(self, 'otter.metrics.get_scaling_groups',
                                        return_value=succeed(self.groups))

        self.metrics = [GroupMetrics('t', 'g1', 3, 2, 0),
                        GroupMetrics('t2', 'g1', 4, 4, 1),
                        GroupMetrics('t2', 'g', 100, 20, 0)]
        self.get_all_metrics = patch(self, 'otter.metrics.get_all_metrics',
                                     return_value=succeed(self.metrics))

        self.add_to_cloud_metrics = patch(self, 'otter.metrics.add_to_cloud_metrics',
                                          return_value=succeed(None))

        self.config = {'cassandra': 'c', 'identity': identity_config, 'metrics': 'm',
                       'region': 'r', 'services': {'cloudServersOpenStack': 'cloudServersOpenStack'}}

    def test_metrics_collected(self):
        """
        Metrics is collected after getting groups from cass and servers from nova
        and it is added to blueflood
        """
        _reactor = mock.Mock()
        d = collect_metrics(_reactor, self.config)
        self.assertIsNone(self.successResultOf(d))

        self.connect_cass_servers.assert_called_once_with(_reactor, 'c')
        self.get_scaling_groups.assert_called_once_with(
            self.client, props=['status'], group_pred=IsCallable())
        self.get_all_metrics.assert_called_once_with(
            self.groups, matches(Provides(IAuthenticator)), {'cloudServersOpenStack': 'cloudServersOpenStack'}, 'r',
            clock=_reactor, _print=False)
        self.add_to_cloud_metrics.assert_called_once_with(
            'm', identity_config['url'], 'r', 107, 26, 1)
        self.client.disconnect.assert_called_once_with()

    def test_with_client(self):
        """
        Uses client provided and does not disconnect it before returning
        """
        client = mock.Mock(spec=['disconnect'])
        d = collect_metrics(mock.Mock(), self.config, client=client)
        self.assertIsNone(self.successResultOf(d))
        self.assertFalse(self.connect_cass_servers.called)
        self.assertFalse(client.disconnect.called)

    def test_with_authenticator(self):
        """
        Uses authenticator provided instead of creating new
        """
        _reactor, auth = mock.Mock(), mock.Mock()
        d = collect_metrics(_reactor, self.config, authenticator=auth)
        self.assertIsNone(self.successResultOf(d))
        self.get_all_metrics.assert_called_once_with(
            self.groups, auth, {'cloudServersOpenStack': 'cloudServersOpenStack'},
            'r', clock=_reactor, _print=False)


class APIOptionsTests(SynchronousTestCase):
    """
    Test the various command line options.
    """

    def test_config_options(self):
        """
        File given in --config option is parsed and its contents are added to `Options`
        object
        """
        config = Options()
        config.open = mock.Mock(return_value=StringIO(u'{"a": "b"}'))
        config.parseOptions(['--config=file.json'])
        self.assertEqual(config, {'a': 'b', 'config': 'file.json',
                                  'services': {'cloudServersOpenStack': 'cloudServersOpenStack'}})


class ServiceTests(SynchronousTestCase):
    """
    Tests for :func:`otter.metrics.makeService` and :class:`otter.metrics.MetricsService`
    """

    def setUp(self):
        """
        Mock cass connection and authenticator
        """
        self.client = mock.Mock(spec=['disconnect'])
        self.client.disconnect.return_value = succeed('disconnected')
        self.mock_ccs = patch(self, 'otter.metrics.connect_cass_servers', return_value=self.client)
        self.config = {'cassandra': 'c', 'identity': identity_config, 'metrics': {'interval': 20}}
        self.mock_cm = patch(self, 'otter.metrics.collect_metrics', return_value=succeed(None))
        self.log = mock_log()
        self.clock = Clock()

    def _service(self):
        return MetricsService('r', self.config, self.log, self.clock)

    def _cm_called(self, calls):
        self.assertEqual(len(self.mock_cm.mock_calls), calls)
        self.mock_cm.assert_called_with('r', self.config, client=self.client,
                                        authenticator=matches(Provides(IAuthenticator)))

    @mock.patch('otter.metrics.MetricsService')
    def test_make_service(self, mock_ms):
        """
        MetricsService is returned with config
        """
        c = {'a': 'v'}
        s = makeService(c)
        self.assertIs(s, mock_ms.return_value)
        from otter.metrics import metrics_log
        mock_ms.assert_called_once_with(matches(IsInstance(ReactorBase)), c, metrics_log)

    def test_collect_metrics_called_again(self):
        """
        `collect_metrics` is called again based on interval given in config
        """
        s = self._service()
        s.startService()
        self.assertTrue(s.running)
        self._cm_called(1)
        self.clock.advance(20)
        self._cm_called(2)

    def test_collect_metrics_called_again_on_error(self):
        """
        `collect_metrics` is called again even if one of the previous call fails
        """
        s = self._service()
        self.mock_cm.return_value = fail(ValueError('a'))
        s.startService()
        self._cm_called(1)
        self.log.err.assert_called_once_with(CheckFailure(ValueError))
        # Service is still running
        self.assertTrue(s.running)
        # And collect_metrics is called again after interval is passed
        self.clock.advance(20)
        self._cm_called(2)

    def test_stop_service(self):
        """
        Client is disconnected when service is stopped
        """
        s = self._service()
        s.startService()
        self.assertTrue(s.running)
        self._cm_called(1)
        d = s.stopService()
        self.assertEqual(self.successResultOf(d), 'disconnected')
        self.assertFalse(s.running)
        # collect_metrics is not called again
        self.clock.advance(20)
        self._cm_called(1)
