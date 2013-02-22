#!/usr/bin/env python
import atexit
import os
import os.path
import shutil
import subprocess
import tempfile
import threading
import time
import weakref


class WeakMethodPartial(object):
    def __init__(self, instance, method_name, *args, **kwargs):
        self.referenced = True
        self.instance_ref = weakref.ref(instance, self._dereferenced)

        # These are not weak references, but we do release them
        # if the instance is deleted.
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs

    def _dereferenced(self, also_self):
        self.referenced = False

        del self.method_name
        del self.args
        del self.kwargs

    def __call__(self, *args, **kwargs):
        if not self.referenced:
            return

        instance = self.instance_ref()

        cur_args = self.args + args

        cur_kwargs = dict(self.kwargs)
        cur_kwargs.update(args)

        return getattr(instance, self.method_name)(*cur_args, **cur_kwargs)


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
        self.stop(except_unless_running=False)

    def start(self, except_if_started=True):
        if self.started:
            if except_if_started:
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
        atexit.register(WeakMethodPartial(self, "stop", except_unless_running=False))

    def stop(self, except_unless_running=True, blocking=False):
        if not self.running:
            if except_unless_running:
                raise Exception("Server is not running.")
            else:
                return

        self.process.terminate()

        # Cleanup has a race condition due to the Postgres process
        # and this one trying to delete the same files in the same
        # directory at once.
        #
        # To avoid this, we must wait for Postgres to quit.
        # Waiting for Postgres to quit while their are open
        # connections in the same thread may cause a deadlock,
        # so the default behaviour is to wait and perform the
        # cleanup in another thread.

        if blocking:
            self._stop_clean()
        else:
            threading.Thread(target=self._stop_clean).start()

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
            if ex.pgcode == "42P04":
                pass # database already exists
            else:
                raise
        finally:
            prep_connection.close()

        connection = psycopg2.connect(
            host=self.socket_dir,
            database=database
        )

        return connection
