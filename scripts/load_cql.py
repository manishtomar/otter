#!/usr/bin/env python

"""
Loads cql into Cassandra
"""
from __future__ import print_function

import argparse
import re
import sys
from pprint import pprint

from cql.apivalues import ProgrammingError
from cql.connection import connect

from effect.twisted import perform

from silverberg.client import CQLClient

from twisted.internet import task
from twisted.internet.endpoints import clientFromString

from otter.effect_dispatcher import get_cql_dispatcher
from otter.models.cass import CassScalingGroupCollection
from otter.test.resources import CQLGenerator


the_parser = argparse.ArgumentParser(description="Load data into Cassandra.")


the_parser.add_argument(
    'cql_dir', type=str, metavar='cql_dir',
    help='Directory containing *.cql files to merge and replace.')

the_parser.add_argument(
    '--webhook-migrate', action='store_true',
    help='Migrate webhook indexes to table')

the_parser.add_argument(
    '--webhook-index-only', action='store_true',
    help='List webhook from indexes that is not there in webhook_keys table')

the_parser.add_argument(
    '--keyspace', type=str, default='otter',
    help='The name of the keyspace.  Default: otter')

the_parser.add_argument(
    '--replication', type=int, default=1,
    help='Replication factor to use if creating the keyspace.  Default: 1')

the_parser.add_argument(
    '--ban-unsafe', action='store_true',
    help=('Whether to check for unsafe instructions ("alter", "drop", '
          '"truncate", and "delete", currently)'))

the_parser.add_argument(
    '--dry-run', action='store_true',
    help="If this option is passed, nothing actually gets loaded into cassandra.")

the_parser.add_argument(
    '--host', type=str, default='localhost',
    help='The host of the cluster to connect to. Default: localhost')

the_parser.add_argument(
    '--port', type=int, default=9160,
    help='The port of the cluster to connect to. Default: 9160')

the_parser.add_argument(
    '--outfile', type=argparse.FileType('w'),
    help=('The output file to write the generated CQL to.  If none is '
          'given, no file will be written to.'))

the_parser.add_argument(
    '--verbose', '-v', action='count', default=0, help="How verbose to be")


def generate(args):
    """
    Generate CQL and/or load it into a cassandra instance/cluster.
    """
    try:
        generator = CQLGenerator(args.cql_dir, safe_only=args.ban_unsafe)
    except Exception as e:
        print(e.message)
        sys.exit(1)

    cql = generator.generate_cql(
        keyspace_name=args.keyspace,
        replication_factor=args.replication,
        outfile=args.outfile)

    if args.dry_run:
        return

    # filter out comments, to make debugging easier
    cql = "\n".join(
        [line for line in cql.split('\n')
         if line.strip() and not line.strip().startswith('--')])

    # no blank lines or pointless whitespace
    commands = [x.strip() for x in cql.split(';') if x.strip()]

    # connect
    if args.verbose > 0:
        print("Attempting to connect to {0}:{1}".format(args.host, args.port))
    try:
        connection = connect(args.host, args.port, cql_version='3.0.4')
    except Exception as e:
        print("CONNECTION ERROR: {0}".format(e.message))
        sys.exit(1)
    cursor = connection.cursor()

    # execute commands
    execute_commands(cursor, commands, args.verbose)

    if args.verbose > 0:
        print('\n----\n')
        print("Done.  Disconnecting.")

    cursor.close()
    connection.close()


def execute_commands(cursor, commands, verbose):
    """
    Execute commands
    """
    for command in commands:
        try:
            cursor.execute(command, {})
        except ProgrammingError as pe:
            # if somewhat verbose, then print(out all errors.)
            # if less verbose, print out only non-already-existing errors
            message = pe.message.lower()
            significant_error = (
                "already exist" not in message and
                "existing keyspace" not in message and
                "existing column" not in message and
                not re.search("index '.*' could not be found", message))

            if verbose > 1 or significant_error:
                print('\n----\n')
                print(command)
                print("{0}".format(pe.message.strip()))

            if significant_error:
                sys.exit(1)

        else:
            # extremely verbose - notify that command executed correctly.
            if args.verbose > 2:
                print('\n----\n')
                print(command)
                print("Ok.")


def setup_connection(reactor, args):
    """
    Return Cassandra connection
    """
    return CQLClient(
        clientFromString(reactor, 'tcp:{}:{}'.format(args.host, args.port)),
        args.keyspace)


def webhook_index(reactor, args):
    """
    Show webhook indexes that is not there table connection
    """
    store = CassScalingGroupCollection(None, None)
    eff = store.get_webhook_index_only()
    conn = setup_connection(reactor, args)
    return perform(get_cql_dispatcher(reactor, conn), eff).addCallback(
        pprint).addCallback(lambda _: conn.disconnect())


def webhook_migrate(reactor, args):
    """
    Migrate webhook indexes to table
    """
    store = CassScalingGroupCollection(None, None)
    eff = store.get_webhook_index_only().on(store.add_webhook_keys)
    conn = setup_connection(reactor, args)
    return perform(get_cql_dispatcher(reactor, conn), eff).addCallback(
        lambda _: conn.disconnect())


def migrate_status(reactor, args):
    """
    Change all DISABLED groups to ERROR and None groups to ACTIVE
    """
    groups = yield Effect(
        CQLQueryExecute(
            query='SELECT status FROM scaling_group;', params={},
            consistency_level=ConsistencyLevel.ONE))
    set_status_q = (
        'UPDATE scaling_group SET status=\'{status}\' '
        'WHERE "tenantId"=:{i}tenantId AND "groupId"=:{i}groupId;')
    queries, params = [], {}
    for i, group in enumerate(groups):
        if group['status'] is None:
            queries.append(set_status_q.format(status='ACTIVE', i=i))
            params['{}tenantId'.format(i)] = group['tenantId']
            params['{}groupId'.format(i)] = group['groupId']
        elif group['status'] == 'DISABLED':
            queries.append(set_status_q.format(status='ERROR', i=i))
            params['{}tenantId'.format(i)] = group['tenantId']
            params['{}groupId'.format(i)] = group['groupId']
    yield Effect(
        CQLQueryExecute(
            query=batch(queries), params=params,
            consistency_level=ConsistencyLevel.ONE))
    do_return(None)


def perform_cql_effect(reactor, conn, eff):
    d = perform(get_cql_dispatcher(reactor, conn), eff)
    return d.addCallback(lambda _: conn.disconnect())


def run(args):
    cqlargs = ['webhook_migrate', 'webhook_index_only', 'migrate_status']
    for cqlarg in cqlargs:
        argval = getattr(args, cqlarg, None)
        if argval:
            eff = globals()[cqlarg]()
            conn = setup_connection(reactor, args)
            task.react(perform_cql_effect, (conn, eff))
    else:
        generate(args)


args = the_parser.parse_args()
run(args)
