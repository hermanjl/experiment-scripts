import os
import time
import run.litmus_util as lu
import shutil as sh
from operator import methodcaller

class ExperimentException(Exception):
    '''Used to indicate when there are problems with an experiment.'''
    def __init__(self, name):
        self.name = name

class ExperimentDone(ExperimentException):
    '''Raised when an experiment looks like it's been run already.'''
    def __str__(self):
        return "Experiment finished already: %d" % self.name

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
        self.tracer_types = tracer_types

    def __setup_tracers(self):
        tracers = [ t(self.working_dir) for t in self.tracer_types ]

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
        if lu.waiting_tasks():
            released = lu.release_tasks()
            self.log("Re-released %d tasks" % released)

            time.sleep(1)

        self.log("Killing all tasks")
        for e in self.executables:
            try:
                e.kill()
            except:
                pass

        time.sleep(1)

    def __strip_path(self, path):
        '''Shorten path to something more readable.'''
        file_dir = os.path.split(self.working_dir)[0]
        if path.index(file_dir) == 0:
            path = path[len(file_dir):]

        return path.strip("/")

    def __check_tasks_status(self):
        '''Raises an exception if any tasks have failed.'''
        msgs = []

        for e in self.executables:
            status = e.poll()
            if status != None and status:
                err_msg = "Task %s failed with status: %s" % (e.wait(), status)
                msgs += [err_msg]

        if msgs:
            # Show at most 3 messages so that every task failing doesn't blow
            # up the terminal
            if len(msgs) > 3:
                num_errs = len(msgs) - 3
                msgs = msgs[0:4] + ["...%d more task errors..." % num_errs]

            out_name = self.__strip_path(self.exec_out.name)
            err_name = self.__strip_path(self.exec_err.name)
            help = "Check dmesg, %s, and %s" % (out_name, err_name)

            raise Exception("\n".join(msgs + [help]))

    def __wait_for_ready(self):
        self.log("Sleeping until tasks are ready for release...")

        wait_start = time.time()
        num_ready  = lu.waiting_tasks()

        while num_ready < len(self.executables):
            # Quit if too much time passes without a task becoming ready
            if time.time() - wait_start > 180.0:
                s = "waiting: %d, submitted: %d" %\
                  (lu.waiting_tasks(), len(self.executables))
                raise Exception("Too much time spent waiting for tasks! %s" % s)

            time.sleep(1)

            # Quit if any tasks fail
            self.__check_tasks_status()

            # Reset the waiting time whenever more tasks become ready
            now_ready = lu.waiting_tasks()
            if now_ready != num_ready:
                wait_start = time.time()
                num_ready  = lu.now_ready

    def __run_tasks(self):
        self.log("Starting %d tasks" % len(self.executables))

        for i,e in enumerate(self.executables):
            try:
                e.execute()
            except:
                raise Exception("Executable failed to start: %s" % e)

        self.__wait_for_ready()

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

    def __to_linux(self):
        msgs = []

        sched = lu.scheduler()
        if sched != "Linux":
            try:
                lu.switch_scheduler("Linux")
            except:
                msgs += ["Scheduler is %s, cannot switch to Linux!" % sched]

        running = lu.all_tasks()
        if running:
            msgs += ["%d real-time tasks still running!" % running]

        if msgs:
            raise SystemCorrupted("\n".join(msgs))

    def __setup(self):
        self.__make_dirs()
        self.__assign_executable_cwds()
        self.__setup_tracers()

        self.log("Writing %d proc entries" % len(self.proc_entries))
        map(methodcaller('write_proc'), self.proc_entries)

        self.log("Starting %d regular tracers" % len(self.regular_tracers))
        map(methodcaller('start_tracing'), self.regular_tracers)

        time.sleep(1)

        self.log("Switching to %s" % self.scheduler)
        lu.switch_scheduler(self.scheduler)

        time.sleep(1)

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

    def log(self, msg):
        print("[Exp %s]: %s" % (self.name, msg))

    def run_exp(self):
        self.__to_linux()

        succ = False
        try:
            self.__setup()

            try:
                self.__run_tasks()
                self.log("Saving results in %s" % self.finished_dir)
                succ = True
            except Exception as e:
                # Give time for whatever failed to finish failing
                time.sleep(2)
                self.__kill_all()

                raise e
            finally:
                self.__teardown()
        finally:
            self.log("Switching back to Linux scheduler")
            self.__to_linux()

        if succ:
            self.__save_results()
            self.log("Experiment done!")
