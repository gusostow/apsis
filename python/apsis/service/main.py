import asyncio
import argparse
import logging
from   pathlib import Path
import sanic
import sanic.response
import sanic.router
import time
import websockets

from   . import api
from   . import DEFAULT_PORT
from   .. import testing
from   ..apsis import Apsis
from   ..jobs import JobsDir
from   ..sqlite import SqliteDB

#-------------------------------------------------------------------------------

LOG_FORMATTER = logging.Formatter(
    fmt="%(asctime)s %(name)-18s [%(levelname)-7s] %(message)s",
    datefmt="%H:%M:%S",
)
LOG_FORMATTER.converter = time.gmtime  # FIXME: Use cron.Time?

#-------------------------------------------------------------------------------

class QueueHandler(logging.Handler):
    """
    Publishes formatted log messages to registered async queues.
    """

    def __init__(self, formatter=None):
        if formatter is None:
            formatter = logging.Formatter()

        super().__init__()
        self.__formatter = formatter
        self.__queues = []


    def register(self) -> asyncio.Queue:
        """
        Returns a new queue, to which log records will be published.
        """
        queue = asyncio.Queue()
        self.__queues.append(queue)
        return queue


    def unregister(self, queue):
        """
        Removes a previously registered queue.
        """
        self.__queues.remove(queue)


    def emit(self, record):
        data = self.__formatter.format(record)
        for queue in list(self.__queues):
            try:
                queue.put_nowait(data)
            except asyncio.QueueFull:
                pass


WS_HANDLER = QueueHandler(LOG_FORMATTER)

#-------------------------------------------------------------------------------

SANIC_LOG_CONFIG = {
    **sanic.log.LOGGING_CONFIG_DEFAULTS,
    "formatters": {
        "generic": {
            "class": "logging.Formatter",
            "format": "%(asctime)s %(name)-18s [%(levelname)-7s] %(message)s",
            "datefmt": LOG_FORMATTER.datefmt,
        },
        "access": {
            "class": "logging.Formatter",
            "format": "%(asctime)s %(name)-18s [%(levelname)-7s] [%(host)s %(request)s %(status)d %(byte)d] %(message)s",
            "datefmt": LOG_FORMATTER.datefmt,
        },
    }
}    
    
#-------------------------------------------------------------------------------

class Router(sanic.router.Router):
    """
    Extended router that supports a catch-all path for missing pages.
    """

    CATCH_ALL_PATH = "/static/index.html"

    def get(self, request):
        try:
            return super().get(request)
        except sanic.router.NotFound:
            return self._get(self.CATCH_ALL_PATH, request.method, "")



app = sanic.Sanic(__name__, router=Router(), log_config=SANIC_LOG_CONFIG)
app.config.LOGO = None

top_dir = Path(__file__).parents[3]

app.blueprint(api.API, url_prefix="/api/v1")
app.static("/static", str(top_dir / "web"))

@app.websocket("/log")
async def websocket_log(request, ws):
    queue = WS_HANDLER.register()
    try:
        while True:
            try:
                await ws.send(await queue.get())
            except websockets.ConnectionClosed:
                break
    finally:
        WS_HANDLER.unregister(queue)

app.static("/.*", str(top_dir / "web" / "index.html"))

#-------------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().handlers[0].formatter = LOG_FORMATTER
    logging.getLogger().handlers.append(WS_HANDLER)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug", action="store_true", default=False,
        help="run in debug mode")
    # FIXME: Can't use localhost on OSX, where it resolves to an IPV6 address,
    # until we pick up this Sanic fix:
    # https://github.com/channelcat/sanic/pull/1053
    parser.add_argument(
        "--host", metavar="HOST", default="127.0.0.1",
        help="server host address")
    parser.add_argument(
        "--port", metavar="PORT", type=int, default=DEFAULT_PORT,
        help="server port")
    parser.add_argument(
        "--create", action="store_true", default=False,
        help="create a new state database")
    parser.add_argument(
        "--test-runs", metavar="NUM", type=int, default=None,
        help="add NUM test runs")
    parser.add_argument(
        "jobs", metavar="JOBS", 
        help="job directory")
    parser.add_argument(
        "db", metavar="DATABASE",
        help="database file")
    args = parser.parse_args()

    jobs    = JobsDir(args.jobs)
    db      = SqliteDB(args.db, args.create)
    apsis   = Apsis(jobs, db)

    if args.test_runs is not None:
        for t, run in testing.get_test_runs(args.test_runs):
            asyncio.ensure_future(apsis.schedule(t, run))

    loop = asyncio.get_event_loop()

    server = app.create_server(
        host        =args.host,
        port        =args.port,
        debug       =args.debug,
    )
    app.apsis = apsis
    app.running = True
    asyncio.ensure_future(server)

    # Set up the scheduler.
    asyncio.ensure_future(apsis.scheduler_loop())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print()
    finally:
        loop.close()


if __name__ == "__main__":
    main()

