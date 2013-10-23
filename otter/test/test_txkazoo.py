"""
Tests for `txkazoo.py`
"""
from __future__ import print_function
import sys

import mock

from twisted.trial.unittest import TestCase
from twisted.internet import task, defer

from otter.txkazoo import TxKazooClient
from otter.test.utils import patch

from kazoo.client import KazooClient

class TxKazooClientTests(TestCase):
    """
    Tests for `TxKazooClient`
    """

    def setUp(self):
        """
        Mock actual KazooClient
        """
        self.kazoo_client = patch(self, 'otter.txkazoo.KazooClient', spec=KazooClient)
        self.defer_to_thread = patch(self, 'otter.txkazoo.deferToThread')
        self.txkzclient = TxKazooClient(hosts='abc', num_threads=20)

    @mock.patch('otter.txkazoo.reactor')
    def test_init(self, mock_reactor):
        """
        __init__ sets up thread size and creates KazooClient
        """
        self.txkzclient = TxKazooClient(hosts='abc', arg2='12', num_threads=20)
        mock_reactor.suggestThreadPoolSize.assert_called_once_with(20)
        self.kazoo_client.assert_called_with(hosts='abc', arg2='12')
        self.assertEqual(self.txkzclient.client, self.kazoo_client.return_value)

    def test_start(self):
        """
        Start is called in thread pool
        """
        d = self.txkzclient.start()
        self.defer_to_thread.assert_called_once_with(self.txkzclient.client.start)
        self.assertEqual(d, self.defer_to_thread.return_value)


@defer.inlineCallbacks
def test_via_cli(reactor, *args):
    client = TxKazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
    yield client.start()
    #path_info = yield client.get('/')
    #yield client.create('/manitest', 'manitest data')
    #new_path = yield client.get('/manitest')
    #print(new_path)
    lock = client.Lock('/manitest_lock')
    yield lock.acquire()
    print('acquired lock')
    yield lock.release()
    print('released lock')
    yield client.stop()

if __name__ == '__main__':
    task.react(test_via_cli, sys.argv[1:])
