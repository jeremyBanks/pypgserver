#!/usr/bin/env python
import psycopg2
import shutil
import tempfile
import unittest

import pgserver

class Test(unittest.TestCase):
    def test_connecting_multiple_times(self):
        pg = pgserver.PostgresServer()

        pg.psycopg2_connect().close()
        pg.psycopg2_connect().close()
        pg.psycopg2_connect().close()
        pg.psycopg2_connect().close()
        pg.psycopg2_connect().close()

    def test_multiple_databases(self):
        pg = pgserver.PostgresServer()

        db1_con = pg.psycopg2_connect("db1")
        db1_cur = db1_con.cursor()
        db1_cur.execute("CREATE TABLE t (x int)")
        db1_cur.execute("INSERT INTO t VALUES (1)")
        db1_con.commit()
        db1_con.close()

        db2_con = pg.psycopg2_connect("db2")
        db2_cur = db2_con.cursor()
        db2_cur.execute("CREATE TABLE t (x int)")
        db2_cur.execute("INSERT INTO t VALUES (2)")
        db2_con.commit()
        db2_con.close()


        # using new connections

        db1_con = pg.psycopg2_connect("db1")
        db1_cur = db1_con.cursor()
        db1_cur.execute("SELECT x FROM t")
        self.assertEqual(db1_cur.fetchall(), [(1,)])

        db2_con = pg.psycopg2_connect("db2")
        db2_cur = db2_con.cursor()
        db2_cur.execute("SELECT x FROM t")
        self.assertEqual(db2_cur.fetchall(), [(2,)])

    def test_data_persisted(self):
        # Run the server twice with the same data.

        data_dir = tempfile.mkdtemp()

        pg1 = pgserver.PostgresServer(data_dir=data_dir)
        pg1_con = pg1.psycopg2_connect()
        
        pg1_cur = pg1_con.cursor()
        pg1_cur.execute("CREATE TABLE foo (bar int)")
        pg1_cur.execute("INSERT INTO foo VALUES (13)")
        pg1_con.commit()

        pg1_con.close()
        pg1.stop(blocking=True)


        pg2 = pgserver.PostgresServer(data_dir=data_dir)
        pg2_con = pg2.psycopg2_connect()

        pg2_cur = pg2_con.cursor()
        pg2_cur.execute("SELECT bar FROM foo")
        self.assertEqual(pg2_cur.fetchall(), [(13,)])

        pg2_con.close()
        pg2.stop(blocking=True)

        shutil.rmtree(data_dir)
