import os.path
import psycopg2

conn_string = "<PUT CONN_STRING HERE>"


def main():
    print "Connecting to database       ->%s" % (conn_string)

    with psycopg2.connect(conn_string) as conn:
        conn.autocommit = True

        with conn.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS stats.matches;")

                cursor.execute(
                    """CREATE TABLE stats.matches AS SELECT b.filename as filename,
                    b.filerowcount as total_records, b.certaincount as certain_count,
                    b.distinctcount as distinct_count,b.uncertaincount as uncertain_count,
                    to_char(a.pending, 'MM/DD/YYYY HH:MM:SS') as date,
                    EXTRACT(EPOCH FROM (a.finished-a.pending)) * 1000 as duration
                    FROM job_log a
                    join stats.matchUpdateStats b on a.id=b.jobid
                    where a.job_name=<PUT JOB_NAME HERE>;""")

                cursor.execute("DROP TABLE IF EXISTS stats.batch_updates;")
                
                cursor.execute(
                    """CREATE TABLE stats.batch_updates AS SELECT b.filename as filename,
                    b.filerowcount as total_records, b.overwritecount as num_updates,
                    b.newrecordcount as num_inserts, to_char(a.pending, 'MM/DD/YYYY HH:MM:SS') as date,
                    EXTRACT(EPOCH FROM (a.finished-a.pending)) * 1000 as duration
                    FROM job_log a
                    join stats.bulkUpdateStats b on a.id=b.jobid
                    where a.job_name='<PUT JOB_NAME HERE>';""")

if __name__ == "__main__":
    main()


