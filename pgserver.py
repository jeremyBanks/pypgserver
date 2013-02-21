#!/usr/bin/env python
from tempfile import mkdtemp as make_temp_dir
from subprocess import Popen as Process
from shutil import rmtree as remove_tree
from os import mkdir as make_dir, listdir as list_dir
from os.path import join as join_path
from time import sleep

import psycopg2

# sudo sysctl -w kern.sysv.shmall=65536
# sudo sysctl -w kern.sysv.shmmax=16777216

# How does Postgres.app avoid this?

class PostgresServer(object):
    def __init__(self, data_dir=None):
        if data_dir is not None:
            self.data_dir = data_dir
            self.data_dir_is_temp = False
            try:
                must_initialize = not list_dir(self.data_dir)
            except OSError:
                make_dir(self.data_dir)
                must_initialize = True
        else:
            self.data_dir = make_temp_dir()
            must_initialize = True
            self.data_dir_is_temp = True
        
        self.socket_dir = make_temp_dir()

        if must_initialize:
            Process([
                "pg_ctl", "init",
                "-D", self.data_dir
            ]).communicate()

        self.process = Process([
            "postgres",
            "-D", self.data_dir,
            "-h", "",
            "-k", self.socket_dir
        ])

        # Wait for server's socket to appear
        while len(list_dir(self.socket_dir)) == 0:
            sleep(1)

    def connect(self, database="pg"):
        connection = psycopg2.connect(
            host=self.socket_dir,
            database="template1"
        )
        try:
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE \"" + database.replace("\"", "\"\"") + "\";")
        except psycopg2.ProgrammingError:
            "I hope this is because the database already exists!"
        finally:
            connection.close()

        return psycopg2.connect(host=self.socket_dir, database=database)

    def __del__(self):
        self.process.terminate()
        self.process.communicate()
        remove_tree(self.socket_dir)
        if self.data_dir_is_temp:
            remove_tree(self.data_dir)

if __name__ == "__main__":
    pg = PostgresServer("data/")

    connection = pg.connect()
    cursor = connection.cursor()
    
    cursor.execute("CREATE TABLE IF NOT EXISTS foo ( bar int )");
    cursor.execute("INSERT INTO foo( bar ) VALUES ( 2 )")
    cursor.execute("SELECT * FROM foo")   
    print cursor.fetchall()

    connection.commit()

