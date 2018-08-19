import asyncio
import logging
from   ora import now

from   .jobs import Jobs
from   .program import ProgramError, ProgramFailure
from   .runs import Run, Runs
from   .scheduled import ScheduledRuns
from   .scheduler import Scheduler

log = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

class Apsis:
    """
    The gestalt scheduling application.

    Responsible for:

    - Assembling subcomponents:
      - job repo
      - persistent database
      - scheduler
      - scheduled

    - Managing run transitions:
      - handing runs from one component to the next
      - applying and persisting transitions

    - Exposing a high-level API for user run operations

    """

    def __init__(self, jobs, db):
        self.__db = db
        self.jobs = Jobs(jobs, db.job_db)
        self.runs = Runs(db.run_db)
        self.scheduled = ScheduledRuns(db.clock_db, self.__start)
        # For now, expose the output database directly.
        self.outputs = db.output_db
        # Tasks for running jobs currently awaited.
        self.__running_tasks = {}

        # Restore scheduled runs from DB.
        _, scheduled_runs = self.runs.query(state=Run.STATE.scheduled)
        for run in scheduled_runs:
            if run.expected:
                # Expected, scheduled runs should not have been persisted.
                log.error(
                    f"not rescheduling expected, scheduled run {run.run_id}")
            else:
                self.scheduled.schedule(run.times["schedule"], run)

        # Continue scheduling from the last time we handled scheduled jobs.
        # FIXME: Rename: schedule horizon?
        stop_time = db.clock_db.get_time()
        log.info(f"scheduling runs from {stop_time}")
        self.scheduler = Scheduler(self.jobs, self.schedule, stop_time)

        # Set up the scheduler.
        self.__scheduler_task = asyncio.ensure_future(self.scheduler.loop())

        # Reconnect to running runs.
        _, running_runs = self.runs.query(state=Run.STATE.running)
        for run in running_runs:
            assert run.program is not None
            future = run.program.reconnect(run)
            self.__wait(run, future)


    def __get_program(self, run):
        """
        Constructs the program for a run, with arguments bound.
        """
        job = self.jobs.get_job(run.inst.job_id)
        program = job.program.bind({
            "run_id": run.run_id,
            "job_id": run.inst.job_id,
            **run.inst.args,
        })
        return program


    async def __start(self, run):
        if run.program is None:
            run.program = self.__get_program(run)

        try:
            running, coro = await run.program.start(run)

        except ProgramError as exc:
            # Program failed to start.
            self._transition(
                run, run.STATE.error, 
                message =exc.message,
                meta    =exc.meta,
                times   =exc.times,
                outputs =exc.outputs,
            )

        else:
            # Program started successfully.
            self._transition(run, run.STATE.running, **running.__dict__)
            future = asyncio.ensure_future(coro)
            self.__wait(run, future)


    def __wait(self, run, future):
        def done(future):
            try:
                try:
                    success = future.result()
                except asyncio.CancelledError:
                    log.info(
                        f"canceled waiting for run to complete: {run.run_id}")
                    return

            except ProgramFailure as exc:
                # Program ran and failed.
                self._transition(
                    run, run.STATE.failure, 
                    message =exc.message,
                    meta    =exc.meta,
                    times   =exc.times,
                    outputs =exc.outputs,
                )

            except ProgramError as exc:
                # Program failed to start.
                self._transition(
                    run, run.STATE.error, 
                    message =exc.message,
                    meta    =exc.meta,
                    times   =exc.times,
                    outputs =exc.outputs,
                )

            else:
                # Program ran and completed successfully.
                self._transition(
                    run, run.STATE.success,
                    meta    =success.meta,
                    times   =success.times,
                    outputs =success.outputs,
                )

            del self.__running_tasks[run.run_id]

        self.__running_tasks[run.run_id] = future
        future.add_done_callback(done)
        # FIXME: Don't just drop the future?


    def __rerun(self, run):
        """
        Reruns a failed run, if indicated by the job's rerun policy.
        """
        job = self.jobs.get_job(run.inst.job_id)
        if job.reruns.count == 0:
            # No reruns.
            return
        
        # Collect all reruns of this run, including the original run.
        _, runs = self.runs.query(rerun=run.rerun)
        runs = list(runs)

        if len(runs) > job.reruns.count:
            # No further reruns.
            log.info(f"retry max count exceeded: {run.rerun}")
            return

        time = now()

        main_run, = ( r for r in runs if r.run_id == run.rerun )
        if (main_run.times["schedule"] is not None
            and time - main_run.times["schedule"] > job.reruns.max_delay):
            # Too much time has elapsed.
            log.info(f"retry max delay exceeded: {run.rerun}")

        # OK, we can rerun.
        rerun_time = time + job.reruns.delay
        asyncio.ensure_future(self.rerun(run, time=rerun_time))


    # --- Internal API ---------------------------------------------------------

    def _transition(self, run, state, *, outputs={}, **kw_args):
        """
        Transitions `run` to `state`, updating it with `kw_args`.
        """
        time = now()

        # Transition the run object.
        run._transition(time, state, **kw_args)

        # Persist outputs.
        # FIXME: We are persisting runs assuming all are new.  This is only
        # OK for the time being because outputs are always added on the final
        # transition.  In general, we have to persist new outputs only.
        for output_id, output in outputs.items():
            self.__db.output_db.add(run.run_id, output_id, output)
            
        # Persist the new state.  
        self.runs.update(run, time)

        if state == run.STATE.failure:
            self.__rerun(run)


    # --- API ------------------------------------------------------------------

    async def schedule(self, time, run):
        """
        Adds and schedules a new run.

        :param time:
          The schedule time at which to run the run.  If `None`, the run
          is run now, instead of scheduled.
        """
        self.runs.add(run)
        if time is None:
            await self.__start(run)
        else:
            self.scheduled.schedule(time, run)
            self._transition(run, run.STATE.scheduled, times={"schedule": time})


    async def cancel(self, run):
        """
        Cancels a scheduled run.

        Unschedules the run and sets it to the error state.
        """
        self.scheduled.unschedule(run)
        self._transition(run, run.STATE.error, message="cancelled")


    async def start(self, run):
        """
        Starts immediately a scheduled run.
        """
        # FIXME: Race conditions?
        self.scheduled.unschedule(run)
        await self.__start(run)


    async def rerun(self, run, *, time=None):
        """
        Creates a rerun of `run`.

        :param time:
          The time at which to schedule the rerun.  If `None`, runs the rerun
          immediately.
        """
        # Create the new run.
        log.info(f"rerun: {run.run_id} at {time or 'now'}")
        rerun = run.run_id if run.rerun is None else run.rerun
        new_run = Run(run.inst, rerun=rerun)
        await self.schedule(time, new_run)
        return new_run


    async def shut_down(self):
        log.info("shutting down")

        for run_id, task in self.__running_tasks.items():
            if task.cancelled():
                log.info(f"task for {run_id} already cancelled")
            else:
                log.info(f"canceling task for {run_id}")
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                log.info(f"task for {run_id} canceled successfully")

        self.__scheduler_task.cancel()
        try:
            await self.__scheduler_task
        except asyncio.CancelledError:
            log.info("scheduler canceled")

        log.info("done shutting down")
        asyncio.get_event_loop().stop()
        


