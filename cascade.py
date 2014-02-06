#!/usr/bin/env python
import os
import sys
import psycopg2
import networkx as nx
from collections import namedtuple
from collections import defaultdict
from argparse import ArgumentParser
from config import TROUBLE, UNLOADABLES, DBNAME, DBHOST
from sql import PKEY_QUERY, FKEY_QUERY


FKEY_DCT = defaultdict(list)
PKEY_DCT = defaultdict(list)
Edge = namedtuple('Edge', "name table column fk_table fk_column")
G = nx.DiGraph()


class Unloadable(object):
    '''Represents a Database entry destined to be deleted'''
    cursor, commit, verbose = None, False, False

    def __init__(self, table, uid):
        '''_self.deps_ is a list of FKEY relations which may have entries pointing to _self_'''
        self.uid, self.table, self.commit = uid, table, commit
        dependencies = G.predecessors(table)
        self.deps = []
        self.queue = []
        for parent in sorted(dependencies, key=topological):
            self.deps += FKEY_DCT[parent, table]

    def unload(self):
        '''Recursively unload this entry by unloading any dependent entries before
        deleting itself from the database. Resolve any cyclic dependencies on the way'''
        for edge in self.deps:
            if edge.name in TROUBLE:
                self.unlink(edge)
            else:
                uids = self.getdeps(edge)
                self.scattergun_delete(edge)  # PKEY HACK
                for uid in uids:
                    ul = Unloadable(edge.table, uid)
                    ul.unload()
                    ul.delete()

    def getdeps(self, edge):
        '''Selects entries from the table that has a fkey pointing to _self.table_'''
        pkey_fieldnames = PKEY_DCT[edge.table]
        selectme = ','.join(pkey_fieldnames)
        query = 'SELECT %s FROM %s WHERE %s'
        values = (selectme, edge.table, self.fkey_referrer(edge))
        if selectme != '':
            self.cursor.execute(query % values)
            ids = [dict(zip(pkey_fieldnames, r)) for r in self.cursor.fetchall()]
            return ids
        return []

    def unlink(self, edge):
        '''Deal with graph cycles by setting to NULL any TROUBLE foreign keys'''
        where = self.fkey_referrer(edge)
        self.execute('UPDATE %s SET %s=NULL WHERE %s;' % (edge.table, edge.column, where))

    def scattergun_delete(self, edge):
        '''PKEY HACK: _self.queue_ is a list of sql statements that get
        executed right before deleting _self_ from the DB'''
        where = self.fkey_referrer(edge)
        self.queue += ['DELETE FROM %s WHERE %s;' % (edge.table, where)]

    def delete(self):
        for query in self.queue:
            self.execute(query)  # PKEY HACK
        self.execute("DELETE FROM %s WHERE %s;" % (self.table, self.pkey_equals_uid()))

    def execute(self, query):
        if self.verbose:
            print query
        if self.commit:
            self.cursor.execute(query)

    def fkey_referrer(self, edge):
        '''Returns a SQL WHERE clause that identifies entries from _edge.table_ which
        point to the _self_ Unloadable through the FKEY relation _edge_'''
        assert edge.fk_table == self.table
        assert len(self.uid) == 1  # TODO - support PKEYs consisting of multiple fields
        return "%s=%s" % (edge.column, self.uid.values()[0])

    def pkey_equals_uid(self):
        '''Returns a SQL WHERE clause that identifies the _self_ Unloadable'''
        clauses = ["%s=%s" % (f, self.uid[f]) for f in PKEY_DCT[self.table]]
        return ' AND '.join(clauses)


if __name__ == "__main__":
    ap = ArgumentParser(description="Delete from database as if it had 'on delete cascade'")
    ap.add_argument('uid', type=int, help="Database primary key value")
    ap.add_argument('-m', '--mode', type=str, choices=UNLOADABLES, help="Database table to unload from")
    ap.add_argument('-d', '--dryrun', action='store_true', default=False, help="Dry run (don't commit anything)")
    ap.add_argument('-v', '--verbose', action='store_true', default=False, help="Print out all the SQL statements")

    args = ap.parse_args()
    commit = not args.dryrun

    user = os.getenv('USER')
    cursor = psycopg2.connect(database=DBNAME, host=DBHOST, user=user).cursor()

    # FIXME (PKEY HACK) - Some primary keys are missing from the results returned
    # by this query (see for example 'pagemembers')
    cursor.execute(PKEY_QUERY)
    for r in cursor.fetchall():
        table, constr, col, pos = r
        if col is not None:
            PKEY_DCT[table] += [col]

    field = PKEY_DCT[args.mode]
    assert len(field) == 1

    cursor.execute(FKEY_QUERY)
    relations = [Edge(*row) for row in cursor.fetchall()]

    for r in relations:
        FKEY_DCT[r.table, r.fk_table] += [r]

    all_edges = [(r.table, r.fk_table) for r in relations]
    G.add_edges_from(all_edges)

    # Get the topological sort order (i.e. entries from which tables need be deleted first)
    dag = nx.DiGraph()
    dag_edges = [(r.table, r.fk_table) for r in relations if not r.name in TROUBLE]
    dag.add_edges_from(dag_edges)
    assert nx.is_directed_acyclic_graph(dag)
    order = nx.topological_sort(dag)
    order_dct = dict([(t, i) for i, t in enumerate(order)])
    topological = lambda table: order_dct[table]

    Unloadable.cursor, Unloadable.commit, Unloadable.verbose = cursor, commit, args.verbose
    ul = Unloadable(args.mode, {field[0]: args.uid})
    try:
        ul.unload()
        ul.delete()
    except KeyboardInterrupt:
        print >> sys.stderr, "Interrupted by user"
        commit = False

    if commit:
        cursor.connection.commit()
        cursor.connection.close()
    else:
        print >> sys.stderr, "WARNING!!! Did not commit anything"
