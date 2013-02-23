#!/usr/bin/env/python
import atexit
import os
import os.path
import shutil
import signal
import subprocess
import tempfile
import threading
import time

from utils import WeakMethodPartial


class PostgresServer(object):
    DEFAULT_DB_NAME = "data"

    def __init__(self, data_dir=None, silence=False, start=True):
        self.started = False # started yet
        self.running = False # not stopped yet

        if data_dir is not None:
            self.data_dir = data_dir
            self.data_dir_is_temp = False
            try:
                must_initialize = not os.listdir(self.data_dir)
            except OSError:
                os.mkdir(self.data_dir)
                must_initialize = True
        else:
            self.data_dir = tempfile.mkdtemp()
            must_initialize = True
            self.data_dir_is_temp = True
        
        self.socket_dir = tempfile.mkdtemp()

        if silence:
            self._process_pipe_kws = lambda: {
                "stdin": open(os.devnull, "r"),
                "stdout": open(os.devnull, "w"),
                "stderr": open(os.devnull, "w"),
            }
        else:
            self._process_pipe_kws = lambda: {
                "stdin": open(os.devnull, "r"),
            }


        if must_initialize:
            subprocess.Popen([
                "pg_ctl", "init",
                "-D", self.data_dir
            ], **self._process_pipe_kws()).communicate()

        if start:
            self.start()

    def __del__(self):
        self.stop(throw_unless_running=False)

    def start(self, throw_if_started=True):
        if self.started:
            if throw_if_started:
                raise Exception("Server may only be started once.")
            else:
                return

        self.process = subprocess.Popen([
            "postgres",
            "-D", self.data_dir,
            "-h", "",
            "-k", self.socket_dir
        ], **self._process_pipe_kws())

        # Wait for server's socket to appear
        while len(os.listdir(self.socket_dir)) == 0:
            time.sleep(0.05)

        self.started = True
        self.running = True

        # If we rely on __del__, then it's possible that something
        # we rely on (such as the subprocess module) may have been
        # deleted.
        atexit.register(WeakMethodPartial(self, "stop", throw_unless_running=False, fast=True, blocking=True))

    def stop(self, throw_unless_running=True, fast=False, blocking=False):
        # Cleanup has a race condition due to the Postgres process
        # and this one trying to delete the same files in the same
        # directory at once.
        #
        # To avoid this, we must wait for Postgres to quit.
        # Waiting for Postgres to quit while their are open
        # connections in the same thread may cause a deadlock,
        # so the default behaviour is to wait and perform the
        # cleanup in another thread.
        #
        # Unfortunately, this could allow Python to terminate before
        # all of Postgres' processes do. For this reason, our atexit
        # handler will block until Postgres is finished. It avoids
        # the deadlock by specifying fast=True to kill current
        # connections instead of waiting for them.

        if not self.running:
            if throw_unless_running:
                if self.started:
                    raise Exception("Server has not been started.")
                else:
                    raise Exception("Server has been stopped.")
            else:
                if self._clean_thread:
                    self._clean_thread.join()
                return

        # http://www.postgresql.org/docs/8.4/static/server-shutdown.html
        if fast:
            self.process.send_signal(signal.SIGINT)
        else:
            self.process.send_signal(signal.SIGTERM)

        if blocking:
            self._stop_clean()
            self._clean_thread = None
        else:
            self._clean_thread = threading.Thread(target=self._stop_clean).start()

        self.running = False

    def _stop_clean(self):
        self.process.communicate()

        shutil.rmtree(self.socket_dir)

        if self.data_dir_is_temp:
            shutil.rmtree(self.data_dir)

    def psycopg2_connect(self, database=None):
        import psycopg2

        database = database or self.DEFAULT_DB_NAME

        prep_connection = psycopg2.connect(
            host=self.socket_dir,
            database="template1"
        )
        try:
            prep_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            prep_cursor = prep_connection.cursor()
            prep_cursor.execute("CREATE DATABASE \"{0}\";".format(
                database.replace("\"", "\"\"")))
        except psycopg2.ProgrammingError as ex:
            if ex.pgcode != "42P04": # database already exists
                raise
        finally:
            prep_connection.close()

        connection = psycopg2.connect(
            host=self.socket_dir,
            database=database
        )

        return connection
