Example usage:

    pg = PostgresServer(data_dir)

    connection = pg.psycopg2_connect()
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS times ( time int )");
    cursor.execute("INSERT INTO times( time ) VALUES ( %s )", [int(time.time())])
    cursor.execute("SELECT time FROM times")   
    print cursor.fetchall()

    connection.commit()
    connection.close()

If you do not specify a `data_dir` it will use a temporary directory.

If you system fails to allocated shared memory,
you may need to increase the limits, as on OS X:

    sudo sysctl -w kern.sysv.shmall=65536
    sudo sysctl -w kern.sysv.shmmax=16777216
