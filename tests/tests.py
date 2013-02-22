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

    def test_data_persisted(self):
        # Run the server twice with the same data.

        data_dir = tempfile.mkdtemp()

        pg1 = pgserver.PostgresServer(data_dir=data_dir)
        pg1_conn = pg1.psycopg2_connect()
        
        pg1_cur = pg1_conn.cursor()
        pg1_cur.execute("CREATE TABLE foo (bar int)")
        pg1_cur.execute("INSERT INTO foo VALUES (13)")
        pg1_conn.commit()

        pg1_conn.close()
        pg1.stop(blocking=True)


        pg2 = pgserver.PostgresServer(data_dir=data_dir)
        pg2_conn = pg2.psycopg2_connect()

        pg2_cur = pg2_conn.cursor()
        pg2_cur.execute("SELECT bar FROM foo")
        self.assertEqual(pg2_cur.fetchall(), [(13,)])

        pg2_conn.close()
        pg2.stop(blocking=True)

        shutil.rmtree(data_dir)
