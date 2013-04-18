import common as com
import os
import time
import run.litmus_util as lu
import shutil as sh
import traceback
from operator import methodcaller

class ExperimentException(Exception):
    '''Used to indicate when there are problems with an experiment.'''
    def __init__(self, name):
        self.name = name


class ExperimentDone(ExperimentException):
    '''Raised when an experiment looks like it's been run already.'''
    def __str__(self):
        return "Experiment finished already: %d" % self.name

class ExperimentFailed(ExperimentException):
    def __str__(self):
        return "Experiment failed during execution: %d" % self.name

class SystemCorrupted(Exception):
    pass

class Experiment(object):
    '''Execute one task-set and save the results. Experiments have unique IDs.'''
    INTERRUPTED_DIR = ".interrupted"

    def __init__(self, name, scheduler, working_dir, finished_dir,
                 proc_entries, executables, tracer_types):
        '''Run an experiment, optionally wrapped in tracing.'''

        self.name = name
        self.scheduler = scheduler
        self.working_dir  = working_dir
        self.finished_dir = finished_dir
        self.proc_entries = proc_entries
        self.executables  = executables
        self.exec_out = None
        self.exec_err = None

        self.task_batch = com.num_cpus()

        self.__make_dirs()
        self.__assign_executable_cwds()
        self.__setup_tracers(tracer_types)


    def __setup_tracers(self, tracer_types):
        tracers = [ t(self.working_dir) for t in tracer_types ]

        self.regular_tracers  = [t for t in tracers if not t.is_exact()]
        self.exact_tracers = [t for t in tracers if t.is_exact()]

        for t in tracers:
            self.log("Enabling %s" % t.get_name())

    def __make_dirs(self):
        interrupted = None

        if os.path.exists(self.finished_dir):
            raise ExperimentDone(self.name)

        if os.path.exists(self.working_dir):
            self.log("Found interrupted experiment, saving in %s" %
                     Experiment.INTERRUPTED_DIR)
            interrupted = "%s/%s" % (os.path.split(self.working_dir)[0],
                                     Experiment.INTERRUPTED_DIR)
            if os.path.exists(interrupted):
                sh.rmtree(interrupted)
            os.rename(self.working_dir, interrupted)

        os.mkdir(self.working_dir)

        if interrupted:
            os.rename(interrupted, "%s/%s" % (self.working_dir,
                                              os.path.split(interrupted)[1]))

    def __assign_executable_cwds(self):
        def assign_cwd(executable):
            executable.cwd = self.working_dir
        map(assign_cwd, self.executables)

    def __kill_all(self):
        # Give time for whatever failed to finish failing
        time.sleep(2)

        released = lu.release_tasks()
        self.log("Re-released %d tasks" % released)

        time.sleep(5)

        self.log("Killing all tasks")
        for e in self.executables:
            try:
                e.kill()
            except:
                pass

        time.sleep(2)

    def __run_tasks(self):
        self.log("Starting %d tasks" % len(self.executables))

        for i,e in enumerate(self.executables):
            try:
                e.execute()
            except:
                raise Exception("Executable failed: %s" % e)

        self.log("Sleeping until tasks are ready for release...")
        start = time.time()
        while lu.waiting_tasks() < len(self.executables):
            if time.time() - start > 30.0:
                s = "waiting: %d, submitted: %d" %\
                  (lu.waiting_tasks(), len(self.executables))
                raise Exception("Too much time spent waiting for tasks! %s" % s)
            time.sleep(1)

        # Exact tracers (like overheads) must be started right after release or
        # measurements will be full of irrelevant records
        self.log("Starting %d released tracers" % len(self.exact_tracers))
        map(methodcaller('start_tracing'), self.exact_tracers)
        time.sleep(1)

        try:
            self.log("Releasing %d tasks" % len(self.executables))
            released = lu.release_tasks()

            if released != len(self.executables):
                # Some tasks failed to release, kill all tasks and fail
                # Need to release non-released tasks before they can be killed
                raise Exception("Released %s tasks, expected %s tasks" %
                                (released, len(self.executables)))

            self.log("Waiting for program to finish...")
            for e in self.executables:
                if not e.wait():
                    raise Exception("Executable %s failed to complete!" % e)

        finally:
            # And these must be stopped here for the same reason
            self.log("Stopping exact tracers")
            map(methodcaller('stop_tracing'), self.exact_tracers)

    def __save_results(self):
        os.rename(self.working_dir, self.finished_dir)

    def log(self, msg):
        print("[Exp %s]: %s" % (self.name, msg))

    def run_exp(self):
        self.__check_system()

        succ = False
        try:
            self.__setup()
            time.sleep(1)

            try:
                self.__run_tasks()
                self.log("Saving results in %s" % self.finished_dir)
                succ = True
            except:
                traceback.print_exc()
                self.__kill_all()
                raise ExperimentFailed(self.name)
            finally:
                self.__teardown()
        finally:
            self.log("Switching to Linux scheduler")

            # Give the tasks 10 seconds to finish before bailing
            start = time.time()
            while lu.all_tasks() > 0:
                if time.time() - start < 10.0:
                    raise SystemCorrupted("%d tasks still running!" %
                                          lu.all_tasks())

            lu.switch_scheduler("Linux")

        if succ:
            self.__save_results()
            self.log("Experiment done!")

    def __check_system(self):
        running = lu.all_tasks()
        if running:
            raise SystemCorrupted("%d tasks already running!" % running)

        sched = lu.scheduler()
        if sched != "Linux":
            raise SystemCorrupted("Scheduler is %s, should be Linux" % sched)

    def __setup(self):
        self.log("Writing %d proc entries" % len(self.proc_entries))
        map(methodcaller('write_proc'), self.proc_entries)
        
        self.log("Starting %d regular tracers" % len(self.regular_tracers))
        map(methodcaller('start_tracing'), self.regular_tracers)

        time.sleep(1)

        self.log("Switching to %s" % self.scheduler)
        lu.switch_scheduler(self.scheduler)

        self.exec_out = open('%s/exec-out.txt' % self.working_dir, 'w')
        self.exec_err = open('%s/exec-err.txt' % self.working_dir, 'w')
        def set_out(executable):
            executable.stdout_file = self.exec_out
            executable.stderr_file = self.exec_err
        map(set_out, self.executables)

    def __teardown(self):
        self.exec_out and self.exec_out.close()
        self.exec_err and self.exec_err.close()

        self.log("Stopping regular tracers")
        map(methodcaller('stop_tracing'), self.regular_tracers)

