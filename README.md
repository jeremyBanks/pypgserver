This allows you to run a Postgres server within your Python process.

I love SQLite's ease of use when I need a database for something
quick, but have become frustrated by its limitations. This is meant
to serve some of the same purpose.

Example usage:

<!-- language: lang-python -->
    
    from pgserver import PostgresServer
    import time

    data_dir = None

    pg = PostgresServer(data_dir)

    connection = pg.psycopg2_connect()
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS times ( time int )");
    cursor.execute("INSERT INTO times( time ) VALUES ( %s )", [int(time.time())])
    cursor.execute("SELECT time FROM times")   
    print cursor.fetchall()

    connection.commit()
    connection.close()

If `data_dir is None` then it will use a temporary directory
as you might use `:memory:` with SQLite.

Multiple processes can interact with the same SQLite database file
simultaneously, but attempting to use the same `data_dir` from
multiple `PostgresServer`s simultaneously is not supported.

If you system fails to allocated shared memory, you may need to increase
the limits, [as on OS X](http://support.apple.com/kb/HT4022):

<!-- language: lang-bash -->

    sudo sysctl -w kern.sysv.shmall=65536
    sudo sysctl -w kern.sysv.shmmax=16777216
