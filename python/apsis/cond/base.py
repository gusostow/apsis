from   apsis.lib.json import TypedJso
from   apsis.runs import template_expand

#-------------------------------------------------------------------------------

class Condition(TypedJso):
    """
    A boolean condition that blocks a run from starting.  The run waits until
    the condition evaluates true.
    """

    TYPE_NAMES = TypedJso.TypeNames()

    # Poll inteval in sec.
    poll_interval = 1

    def bind(self, run, jobs):
        """
        Binds the condition to `inst`.

        :param run:
          The run to bind to.
        :param jobs:
          The jobs DB.
        :return:
          An instance of the same type, bound to the instances.
        """


    async def check(self):
        """
        Returns true if the condition is satisfied.
        """
        return True


    def check_runs(self, run_store):
        """
        Returns true if run conditions are satisfied.
        """
        return True



#-------------------------------------------------------------------------------

def _bind(job, obj_args, inst_args, bind_args):
    """
    Binds args to `job.params`.

    Binds `obj_args` and `inst_args` to params by name.  `obj_args` take
    precedence, and are template-expanded with `bind_args`; `inst_args` are
    not expanded.
    """
    def get(name):
        try:
            return template_expand(obj_args[name], bind_args)
        except KeyError:
            pass
        try:
            return inst_args[name]
        except KeyError:
            pass
        raise LookupError(f"no value for param {name} of job {job.job_id}")

    return { n: get(n) for n in job.params }


