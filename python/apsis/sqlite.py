"""
Persistent state stored in a sqlite file.
"""

import logging
import ora
from   pathlib import Path
import sqlalchemy as sa
import ujson

from   .jobs import jso_to_job, job_to_jso
from   .lib import itr
from   .runs import Instance, Run
from   .program import Program, Output, OutputMetadata

log = logging.getLogger(__name__)

# FIXME: For next version:
# - remove runs.rerun column
# - rename run_history to run_log

#-------------------------------------------------------------------------------

# We store times as float seconds from the epoch.

def dump_time(time):
    return time - ora.UNIX_EPOCH


def load_time(time):
    return ora.UNIX_EPOCH + time


def _get_max_run_id_num(engine, table):
    with engine.begin() as conn:
        rows = list(conn.execute(
            f"SELECT MAX(CAST(SUBSTR(run_id, 2) AS DECIMAL)) FROM {table}"))
    (max_num, ), = rows
    return 0 if max_num is None else max_num


METADATA = sa.MetaData()

#-------------------------------------------------------------------------------

TBL_CLOCK = sa.Table(
    "clock", METADATA, 
    sa.Column("time", sa.Float(), nullable=False),
)


class ClockDB:
    """
    Stores the most recent application time.
    """

    # We use a DB-API connection and SQL statements because it's faster than
    # the SQLAlchemy ORM.

    def __init__(self, engine):
        self.__connection = engine.connect().connection

        (length, ), = self.__connection.execute("SELECT COUNT(*) FROM clock")
        if length == 0:
            time = ora.now() - ora.UNIX_EPOCH
            self.__connection.execute("INSERT INTO clock VALUES (?)", (time, ))
            self.__connection.commit()
        else:
            assert length == 1


    def get_time(self):
        (time, ), = self.__connection.execute("SELECT time FROM clock")
        return time + ora.UNIX_EPOCH


    def set_time(self, time):
        time -= ora.UNIX_EPOCH
        self.__connection.execute("UPDATE clock SET time = ?", (time, ))
        self.__connection.commit()



#-------------------------------------------------------------------------------

TBL_JOBS = sa.Table(
    "jobs", METADATA,
    sa.Column("job_id"        , sa.String()       , nullable=False),
    sa.Column("job"           , sa.String()       , nullable=False),
)

class JobDB:

    def __init__(self, engine):
        self.__engine = engine


    def insert(self, job):
        # FIXME: Check that the job ID doesn't exist already
        with self.__engine.begin() as conn:
            conn.execute(TBL_JOBS.insert().values(
                job_id  =job.job_id,
                job     =ujson.dumps(job_to_jso(job)),
            ))


    def get(self, job_id):
        with self.__engine.begin() as conn:
            query = sa.select([TBL_JOBS]).where(TBL_JOBS.c.job_id == job_id)
            rows = list(conn.execute(query))
            assert len(rows) <= 1

            if len(rows) == 0:
                raise LookupError(job_id)
            else:
                (_, job), = rows
                return jso_to_job(ujson.loads(job), job_id)


    def query(self, *, ad_hoc=None):
        query = sa.select([TBL_JOBS])
        with self.__engine.begin() as conn:
            for job_id, job in conn.execute(query):
                # FIXME: Filter ad hoc jobs in the query.
                try:
                    job = jso_to_job(ujson.loads(job), job_id)
                except Exception as exc:
                    logging.error(f"failed to load job from DB: {exc}")
                    continue
                if ad_hoc is None or job.ad_hoc == ad_hoc:
                    yield job



#-------------------------------------------------------------------------------

# FIXME: For now, we store times and meta as JSON.  To make these searchable,
# we'll need to break them out into tables.

# FIXME: Split out args into a separate table?
# FIXME: Split out instances into a separate table?

# FIXME: Store int enumerals for state?
# FIMXE: Use a TIME column for 'time'?

TBL_RUNS = sa.Table(
    "runs", METADATA,
    sa.Column("rowid"       , sa.Integer()      , primary_key=True),
    sa.Column("run_id"      , sa.String()       , nullable=False),
    sa.Column("timestamp"   , sa.Float()        , nullable=False),
    sa.Column("job_id"      , sa.String()       , nullable=False),
    sa.Column("args"        , sa.String()       , nullable=False),
    sa.Column("state"       , sa.String()       , nullable=False),
    sa.Column("program"     , sa.String()       , nullable=True),
    sa.Column("times"       , sa.String()       , nullable=False),
    sa.Column("meta"        , sa.String()       , nullable=False),
    sa.Column("message"     , sa.String()       , nullable=True),
    sa.Column("run_state"   , sa.String()       , nullable=True),
    sa.Column("rerun"       , sa.String()       , nullable=True),  # FIXME: Unused.
    sa.Column("expected"    , sa.Boolean()      , nullable=True),
)


class RunDB:

    # For runs in the database (either inserted into or loaded from), we stash
    # the sqlite rowid in the Run._rowid attribute.

    def __init__(self, engine):
        self.__engine = engine
        self.__connection = engine.raw_connection()
        # FIXME: Do we need to clean this up?


    @staticmethod
    def __query_runs(conn, expr):
        query = sa.select([TBL_RUNS]).where(expr)
        log.debug(str(query).replace("\n", " "))

        cursor = conn.execute(query)
        for (
                rowid, run_id, timestamp, job_id, args, state, program, times,
                meta, message, run_state, _, _
        ) in cursor:
            if program is not None:
                program     = Program.from_jso(ujson.loads(program))

            times           = ujson.loads(times)
            times           = { n: ora.Time(t) for n, t in times.items() }

            args            = ujson.loads(args)
            inst            = Instance(job_id, args)
            run             = Run(inst)

            run.run_id      = run_id
            run.timestamp   = load_time(timestamp)
            run.state       = Run.STATE[state]
            run.program     = program
            run.times       = times
            run.meta        = ujson.loads(meta)
            run.message     = message
            run.run_state   = ujson.loads(run_state)
            run._rowid      = rowid
            yield run


    def upsert(self, run):
        assert run.run_id.startswith("r")
        assert not run.expected
        rowid = int(run.run_id[1 :])

        program = (
            None if run.program is None
            else ujson.dumps(run.program.to_jso())
        )
        # FIXME: Precos, same as program.

        times = { n: str(t) for n, t in run.times.items() }
        
        # We use SQL instead of SQLAlchemy for performance.
        values = (
            run.run_id,
            dump_time(run.timestamp),
            run.inst.job_id,
            ujson.dumps(run.inst.args),
            run.state.name,
            program,
            ujson.dumps(times),
            ujson.dumps(run.meta),
            run.message,
            ujson.dumps(run.run_state),
            rowid,
        )

        try:
            # Get the rowid; if it's missing, this run isn't in the table.
            run._rowid

        except AttributeError:
            # This run isn't in the database yet.
            # FIXME: sqlite doesn't accepted "FALSE" until version 3.23.0.
            self.__connection.connection.execute("""
                INSERT INTO runs (
                    run_id, 
                    timestamp, 
                    job_id, 
                    args, 
                    state, 
                    program, 
                    times, 
                    meta, 
                    message, 
                    run_state, 
                    rowid,
                    expected
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """, values)
            self.__connection.connection.commit()
            run._rowid = values[-1]

        else:
            # Update the existing row.
            self.__connection.connection.execute("""
                UPDATE runs SET
                    run_id      = ?, 
                    timestamp   = ?, 
                    job_id      = ?, 
                    args        = ?, 
                    state       = ?, 
                    program     = ?, 
                    times       = ?, 
                    meta        = ?, 
                    message     = ?, 
                    run_state   = ?
                WHERE rowid = ?
            """, values)
            self.__connection.connection.commit()


    def get(self, run_id):
        with self.__engine.begin() as conn:
            run, = self.__query_runs(conn, TBL_RUNS.c.run_id == run_id)
        return run


    def query(self, *, job_id=None, since=None, min_timestamp=None):
        """
        :param min_timestamp:
          If not none, limits to runs with timestamp not less than this.
        """
        log.debug(f"query job_id={job_id} since={since}")
        where = []
        if job_id is not None:
            where.append(TBL_RUNS.c.job_id == job_id)
        if since is not None:
            where.append(TBL_RUNS.c.rowid >= int(since))
        if min_timestamp is not None:
            where.append(TBL_RUNS.c.timestamp >= dump_time(min_timestamp))

        with self.__engine.begin() as conn:
            # FIMXE: Return only the last record for each run_id?
            runs = list(self.__query_runs(conn, sa.and_(*where)))

        log.debug(f"query returned {len(runs)} runs")
        return runs


    def get_max_run_id_num(self):
        return _get_max_run_id_num(self.__engine, "runs")



#-------------------------------------------------------------------------------

class RunLogDB:

    TABLE = sa.Table(
        "run_history", METADATA,
        sa.Column("run_id"      , sa.String()   , nullable=False),
        sa.Column("timestamp"   , sa.Float      , nullable=False),
        sa.Column("message"     , sa.String()   , nullable=False),
        sa.Index("idx_run_id", "run_id"),
    )

    def __init__(self, engine):
        self.__engine = engine
        self.__cache = {}


    def cache(self, run_id: str, timestamp: ora.Time, message: str):
        values = {
            "run_id"    : run_id,
            "timestamp" : timestamp,
            "message"   : str(message),
        }
        self.__cache.setdefault(run_id, []).append(values)


    def insert(self, run_id: str, timestamp: ora.Time, message: str):
        values = {
            "run_id"    : run_id,
            "timestamp" : dump_time(timestamp),
            "message"   : str(message),
        }
        with self.__engine.begin() as conn:
            conn.execute(self.TABLE.insert().values(**values))


    def flush(self, run_id):
        """
        FLushes cached run log to the database.
        """
        cache = self.__cache.pop(run_id, ())
        if len(cache) > 0:
            for item in cache:
                item["timestamp"] = dump_time(item["timestamp"])
            with self.__engine.begin() as conn:
                conn.execute(self.TABLE.insert().values(cache))


    def query(self, *, run_id: str):
        log.debug(f"query run log run_id={run_id}")
        where = self.TABLE.c.run_id == run_id

        # Respond with cached values.
        yield from self.__cache.get(run_id, ())

        # Now query the database.
        with self.__engine.begin() as conn:
            rows = list(conn.execute(sa.select([self.TABLE]).where(where)))

        for run_id, timestamp, message in rows:
            yield {
                "run_id"    : run_id,
                "timestamp" : load_time(timestamp),
                "message"   : message,
            }


    def get_max_run_id_num(self):
        return _get_max_run_id_num(self.__engine, "run_history")



#-------------------------------------------------------------------------------

class OutputDB:
    """
    We store even large outputs in the SQLite database, which should generally
    be efficient.  See https://www.sqlite.org/intern-v-extern-blob.html.
    """

    TABLE = sa.Table(
        "output", METADATA,
        sa.Column("run_id"      , sa.String()   , nullable=False),
        sa.Column("output_id"   , sa.String()   , nullable=False),
        sa.Column("name"        , sa.String()   , nullable=False),
        sa.Column("content_type", sa.String()   , nullable=False),
        sa.Column("length"      , sa.Integer()  , nullable=False),
        sa.Column("compression" , sa.String()   , nullable=True),
        sa.Column("data"        , sa.BINARY()   , nullable=False),
        sa.PrimaryKeyConstraint("run_id", "output_id")
    )

    def __init__(self, engine):
        self.__engine = engine


    def add(self, run_id: str, output_id: str, output: Output):
        values = {
            "run_id"        : run_id,
            "output_id"     : output_id,
            "name"          : output.metadata.name,
            "content_type"  : output.metadata.content_type,
            "length"        : output.metadata.length,
            "compression"   : output.compression,
            "data"          : output.data,
        }
        with self.__engine.begin() as conn:
            conn.execute(self.TABLE.insert().values(**values))


    def get_metadata(self, run_id) -> OutputMetadata:
        """
        Returns all output metadata for run `run_id`.

        :return:
          A mapping from output ID to `OutputMetadata` instances.  If no output
          is stored for `run_id`, returns an empty dict.
        """
        cols    = self.TABLE.c
        columns = [cols.output_id, cols.name, cols.content_type, cols.length]
        query   = sa.select(columns).where(cols.run_id == run_id)
        return {
            r[0]: OutputMetadata(name=r[1], length=r[3], content_type=r[2])
            for r in self.__engine.execute(query)
        }


    def get_data(self, run_id, output_id) -> bytes:
        cols = self.TABLE.c
        query = (
            sa.select([cols.compression, cols.data])
            .where((cols.run_id == run_id) & ((cols.output_id == output_id)))
        )
        rows = list(self.__engine.execute(query))
        if len(rows) == 0:
            raise LookupError(f"no output {output_id} for {run_id}")
        else:
            (compression, data), = rows
            if compression is None:
                return data
            else:
                raise NotImplementedError(f"compression: {compression}")



#-------------------------------------------------------------------------------

class SqliteDB:
    """
    A SQLite3 file containing persistent state.
    """

    def __init__(self, engine):
        """
        :param path:
          Path to SQLite file.  If `None`, use a memory DB (for testing).
        """
        self.clock_db       = ClockDB(engine)
        self.job_db         = JobDB(engine)
        self.run_db         = RunDB(engine)
        self.run_log_db     = RunLogDB(engine)
        self.output_db      = OutputDB(engine)
        self._engine        = engine


    @classmethod
    def __get_engine(cls, path):
        url = "sqlite://" if path is None else f"sqlite:///{path}"
        return sa.create_engine(url)


    @classmethod
    def create(cls, path):
        """
        Creates a new database.

        :param path:
          The database path, which must not already exist.
        :return:
          The new database.
        """
        if path is not None:
            path = Path(path).absolute()
            if path.exists():
                raise FileExistsError(path)
        
        engine  = cls.__get_engine(path)
        METADATA.create_all(engine)
        return cls(engine)


    @classmethod
    def migrate(cls, path):
        """
        (Attempts to) migrate the database at `path`.
        """
        assert path is not None
        path = Path(path).absolute()
        if not path.exists():
            raise FileNotFoundError(path)

        engine = cls.__get_engine(path)
        METADATA.create_all(engine)

        # Clean up expected runs; these used to be persisted.
        try:
            engine.execute("DELETE FROM runs WHERE expected")
        except sa.exc.OperationalError:
            # Column may not exist.
            pass


    @classmethod
    def open(cls, path):
        if path is not None:
            path = Path(path).absolute()
            if not path.exists():
                raise FileNotFoundError(path)

        engine  = cls.__get_engine(path)
        # FIXME: Check that tables exist.
        return cls(engine)


    def get_max_run_id_num(self):
        """
        :return:
          The largest run ID number in use, or 0.
        """
        return max(
            self.run_db.get_max_run_id_num(),
            self.run_log_db.get_max_run_id_num(),
        )



#-------------------------------------------------------------------------------

def check(db):
    """
    Checks `db` for consistency.
    """
    ok = True

    def error(msg):
        nonlocal ok
        logging.error(msg)
        ok = False

    engine = db._engine
    run_tables = (RunLogDB.TABLE, OutputDB.TABLE)

    # Check run tables for valid run ID (referential integrity).
    for tbl in run_tables:
        sel = (
            sa.select([tbl.c.run_id])
            .distinct(tbl.c.run_id)
            .select_from(tbl.outerjoin(
                TBL_RUNS,
                tbl.c.run_id == TBL_RUNS.c.run_id
            )).where(TBL_RUNS.c.run_id == None)
        )
        log.debug(f"query:\n{sel}")
        res = engine.execute(sel)
        run_ids = [ r for (r, ) in res ]
        if len(run_ids) > 0:
            error(f"unknown run IDs in {tbl}: {itr.join_truncated(8, run_ids)}")


def archive_runs(db, archive_db, time, *, delete=False):
    """
    Moves runs from `db` to `archive_db` that are older than `time`.
    """
    in_eng = db._engine
    arc_eng = archive_db._engine

    # Tables other than "runs" that need to be archived.
    run_tables = (RunLogDB.TABLE, OutputDB.TABLE)

    # Selection for runs in the runs table itself.
    sel = TBL_RUNS.c.timestamp < dump_time(time)

    def joined(table):
        """
        Returns `table` with matching runs selected.
        """
        return table.join(
            TBL_RUNS,
            sa.and_(table.c.run_id == TBL_RUNS.c.run_id, sel)
        )

    def copy(sel, tbl, chunk_size=16384):
        """
        Copies rows from `sel` to `tbl`.
        """
        # Need to convert row tuples to dicts for sa 1.4.
        # https://github.com/sqlalchemy/sqlalchemy/issues/8010
        result_dicts = (
            dict(zip(sel.c.keys(), tup)) for tup in in_eng.execute(sel)
        )
        for chunk in itr.chunks(result_dicts, chunk_size):
            with arc_eng.begin() as tx:
                tx.execute(sa.insert(tbl), chunk)

    # Copy rows corresponding to these runs in other tables.
    for table in run_tables:
        logging.info(f"copying {table}")
        copy(sa.select(table.c).select_from(joined(table)), table)
    # Copy the runs themselves.
    logging.info("copying runs")
    copy(sa.select(TBL_RUNS.c).where(sel), TBL_RUNS)

    if delete:
        with in_eng.begin() as tx:
            # Delete rows corresponding to these runs in other tables.
            run_sel = sa.select([TBL_RUNS.c.run_id]).where(sel)
            for table in run_tables:
                logging.info(f"deleting {table}")
                tx.execute(table.delete().where(table.c.run_id.in_(run_sel)))
            # Delete the runs themselves.
            logging.info("deleting runs")
            tx.execute(TBL_RUNS.delete().where(sel))

        # Verify.
        for table in run_tables:
            logging.info(f"verifying {table}")
            assert in_eng.execute(
                sa.select([sa.func.count()])
                .select_from(joined(table))
            ).scalar() == 0
        logging.info("verifying runs")
        assert in_eng.execute(
            sa.select([sa.func.count()])
            .where(sel)
        ).scalar() == 0
        
        logging.info("vacuuming")
        in_eng.execute("VACUUM")

