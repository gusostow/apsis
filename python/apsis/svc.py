import asyncio
from   cron import *
import logging

from   apsis import scheduler
import apsis.testing

#-------------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO)

    time = now()
    docket = scheduler.Docket(time)
    scheduler.schedule_insts(docket, apsis.testing.JOBS, time + 1 * 86400)

    event_loop = asyncio.get_event_loop()

    # Set off the handler.
    event_loop.call_soon(scheduler.docket_handler, docket)

    try:
        event_loop.run_forever()
    finally:
        event_loop.close()


if __name__ == '__main__':
    main()

