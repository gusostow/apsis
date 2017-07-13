import asyncio
from   cron import *
from   functools import partial
import getpass
import logging
from   pathlib import Path
import shlex
import socket
import subprocess

from   . import lib
from   .execute import ProgramError, ProgramFailure
from   .job import *

log = logging.getLogger("program")

#-------------------------------------------------------------------------------

class ProcessProgram:

    def __init__(self, argv):
        self.__argv = tuple( str(a) for a in argv )
        self.__executable = Path(argv[0])


    def __str__(self):
        return " ".join( shlex.quote(a) for a in self.__argv )


    def to_jso(self):
        return {
            "argv"      : list(self.__argv),
        }


    @classmethod
    def from_jso(class_, jso):
        return class_(
            jso["argv"],
        )


    async def start(self, run):
        log.info("running: {}".format(run))

        start_time = now()
        run.meta.update({
            "hostname"  : socket.gethostname(),
            "username"  : getpass.getuser(),
            "start_time": str(start_time),
        })

        try:
            with open("/dev/null") as stdin:
                proc = await asyncio.create_subprocess_exec(
                    *self.__argv, 
                    executable  =self.__executable,
                    stdin       =stdin,
                    # Merge stderr with stdin.  FIXME: Do better.
                    stdout      =asyncio.subprocess.PIPE,
                    stderr      =asyncio.subprocess.STDOUT,
                )
        except OSError as exc:
            raise ProgramError(str(exc))
        else:
            return proc


    async def wait(self, run, proc):
        stdout, stderr = await proc.communicate()
        end_time = str(now())
        return_code = proc.returncode

        assert stderr is None
        assert return_code is not None

        run.meta.update({
            "pid"           : proc.pid,
            "return_code"   : return_code,
            "end_time"      : str(end_time),
        })
        run.output = stdout
        if return_code == 0:
            return
        else:
            raise ProgramFailure("return code = {}".format(return_code))



class ShellCommandProgram(ProcessProgram):

    def __init__(self, command):
        # FIXME: Which shell?
        command = str(command)
        super().__init__(["/bin/bash", "-c", command])
        self.__command = command


    def __str__(self):
        return self.__command



#-------------------------------------------------------------------------------

TYPES = (
    ProcessProgram,
    ShellCommandProgram,
)

to_jso      = partial(lib.to_jso, types=TYPES)
from_jso    = partial(lib.from_jso, types=TYPES)

