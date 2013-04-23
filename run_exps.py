#!/usr/bin/env python
from __future__ import print_function

import common as com
import os
import re
import shutil
import sys
import run.tracer as trace

from config.config import PARAMS,DEFAULTS
from collections import namedtuple
from optparse import OptionParser
from run.executable.executable import Executable
from run.experiment import Experiment,ExperimentDone,SystemCorrupted
from run.proc_entry import ProcEntry

'''Customizable experiment parameters'''
ExpParams = namedtuple('ExpParams', ['scheduler', 'duration', 'tracers',
                                     'kernel', 'config_options', 'file_params',
                                     'pre_script', 'post_script'])
'''Comparison of requested versus actual kernel compile parameter value'''
ConfigResult = namedtuple('ConfigResult', ['param', 'wanted', 'actual'])

class InvalidKernel(Exception):
    def __init__(self, kernel):
        self.kernel = kernel

    def __str__(self):
        return "Kernel name does not match '%s'." % self.kernel


class InvalidConfig(Exception):
    def __init__(self, results):
        self.results = results

    def __str__(self):
        rstr = "'%s'%swanted: '%s', found: %s"
        messages = []
        for r in self.results:
            # For pretty alignment
            tabs = (3 - len(r.param)/8)
            messages += [rstr % (r.param, '\t'*tabs, r.wanted, r.actual)]

        return "Invalid kernel configuration " +\
               "(ignore configuration with -i option).\n" + "\n".join(messages)


def parse_args():
    parser = OptionParser("usage: %prog [options] [sched_file]... [exp_dir]...")

    parser.add_option('-s', '--scheduler', dest='scheduler',
                      help='scheduler for all experiments')
    parser.add_option('-i', '--ignore-environment', dest='ignore',
                      action='store_true', default=False,
                      help='run experiments even in invalid environments ')
    parser.add_option('-d', '--duration', dest='duration', type='int',
                      help='duration (seconds) of tasks')
    parser.add_option('-o', '--out-dir', dest='out_dir',
                      help='directory for data output',
                      default=DEFAULTS['out-run'])
    parser.add_option('-p', '--params', dest='param_file',
                      help='file with experiment parameters')
    parser.add_option('-c', '--schedule-file', dest='sched_file',
                      help='name of schedule files within directories',
                      default=DEFAULTS['sched_file'])
    parser.add_option('-f', '--force', action='store_true', default=False,
                      dest='force', help='overwrite existing data')
    parser.add_option('-j', '--jabber', metavar='username@domain',
                      dest='jabber', default=None,
                      help='send a jabber message when an experiment completes')
    parser.add_option('-e', '--email', metavar='username@server',
                      dest='email', default=None,
                      help='send an email when all experiments complete')

    return parser.parse_args()


def convert_data(data):
    '''Convert a non-python schedule file into the python format'''
    regex = re.compile(
        r"(?P<PROC>^"
            r"(?P<HEADER>/proc/[\w\-]+?/)?"
            r"(?P<ENTRY>[\w\-\/]+)"
              r"\s*{\s*(?P<CONTENT>.*?)\s*?}$)|"
        r"(?P<TASK>^"
            r"(?:(?P<PROG>[^\d\-\s][\w\.]*?) )?\s*"
            r"(?P<ARGS>[\w\-_\d\. \=]+)\s*$)",
        re.S|re.I|re.M)

    procs = []
    tasks = []

    for match in regex.finditer(data):
        if match.group("PROC"):
            header = match.group("HEADER") or "/proc/litmus/"
            loc  = "{}{}".format(header, match.group("ENTRY"))
            proc = (loc, match.group("CONTENT"))
            procs.append(proc)
        else:
            prog = match.group("PROG") or DEFAULTS['prog']
            spin = (prog, match.group("ARGS"))
            tasks.append(spin)

    return {'proc' : procs, 'task' : tasks}


def fix_paths(schedule, exp_dir, sched_file):
    '''Replace relative paths of command line arguments with absolute ones.'''
    for (idx, (task, args)) in enumerate(schedule['task']):
        for arg in re.split(" +", args):
            abspath = "%s/%s" % (exp_dir, arg)
            if os.path.exists(abspath):
                args = args.replace(arg, abspath)
                break
            elif re.match(r'.*\w+\.[a-zA-Z]\w*', arg):
                sys.stderr.write("WARNING: non-existent file '%s' " % arg +
                                 "may be referenced:\n\t%s" % sched_file)

        schedule['task'][idx] = (task, args)


def load_schedule(name, fname, duration):
    '''Turn schedule file @fname into ProcEntry's and Executable's which execute
    for @duration time.'''
    with open(fname, 'r') as f:
        data = f.read().strip()
    try:
        schedule = eval(data)
    except:
        schedule = convert_data(data)

    sched_dir = os.path.split(fname)[0]

    # Make paths relative to the file's directory
    fix_paths(schedule, sched_dir, fname)

    proc_entries = []
    executables  = []

    # Create proc entries
    for entry_conf in schedule['proc']:
        proc_entries += [ProcEntry(*entry_conf)]

    # Create executables
    for task_conf in schedule['task']:
        if len(task_conf) != 2:
            raise Exception("Invalid task conf %s: %s" % (task_conf, name))

        (task, args) = (task_conf[0], task_conf[1])

        real_task = com.get_executable(task, sched_dir)

        # Last argument must always be duration
        real_args = args.split() + [duration]

        # All spins take a -w flag
        if re.match(".*spin$", real_task) and '-w' not in real_args:
            real_args = ['-w'] + real_args

        executables += [Executable(real_task, real_args)]

    return proc_entries, executables


def verify_environment(exp_params):
    '''Raise an exception if the current system doesn't match that required
    by @exp_params.'''
    if exp_params.kernel and not re.match(exp_params.kernel, com.kernel()):
        raise InvalidKernel(exp_params.kernel)

    if exp_params.config_options:
        results = []
        for param, wanted in exp_params.config_options.iteritems():
            try:
                actual = com.get_config_option(param)
            except IOError:
                actual = None
            if not str(wanted) == str(actual):
                results += [ConfigResult(param, wanted, actual)]

        if results:
            raise InvalidConfig(results)


def run_script(script_params, exp, exp_dir, out_dir):
    '''Run an executable (arguments optional)'''
    if not script_params:
        return

    # Split into arguments and program name
    if type(script_params) != type([]):
        script_params = [script_params]

    exp.log("Running %s" % script_params.join(" "))

    script_name = script_params.pop(0)
    script = com.get_executable(script_name, cwd=exp_dir)

    out  = open('%s/%s-out.txt' % (out_dir, script_name), 'w')
    prog = Executable(script, script_params, cwd=out_dir,
                      stderr_file=out, stdout_file=out)

    prog.execute()
    prog.wait()

    out.close()


def make_exp_params(cmd_scheduler, cmd_duration, sched_dir, param_file):
    '''Return ExpParam with configured values of all hardcoded params.'''
    kernel = copts = ""

    # Load parameter file
    param_file = param_file or "%s/%s" % (sched_dir, DEFAULTS['params_file'])
    if os.path.isfile(param_file):
        fparams = com.load_params(param_file)
    else:
        fparams = {}

    scheduler = cmd_scheduler or fparams[PARAMS['sched']]
    duration  = cmd_duration  or fparams[PARAMS['dur']] or\
                DEFAULTS['duration']

    # Experiments can specify required kernel name
    if PARAMS['kernel'] in fparams:
        kernel = fparams[PARAMS['kernel']]

    # Or required config options
    if PARAMS['copts'] in fparams:
        copts = fparams[PARAMS['copts']]

    # Or required tracers
    requested = []
    if PARAMS['trace'] in fparams:
        requested = fparams[PARAMS['trace']]
    tracers = trace.get_tracer_types(requested)

    # Or scripts to run before and after experiments
    def get_script(name):
        return fparams[name] if name in fparams else None
    pre_script  = get_script('pre')
    post_script = get_script('post')

    # But only these two are mandatory
    if not scheduler:
        raise IOError("No scheduler found in param file!")
    if not duration:
        raise IOError("No duration found in param file!")

    return ExpParams(scheduler=scheduler, kernel=kernel, duration=duration,
                     config_options=copts, tracers=tracers, file_params=fparams,
                     pre_script=pre_script, post_script=post_script)

def run_experiment(name, sched_file, exp_params, out_dir,
                   start_message, ignore, jabber):
    '''Load and parse data from files and run result.'''
    if not os.path.isfile(sched_file):
        raise IOError("Cannot find schedule file: %s" % sched_file)

    dir_name, fname = os.path.split(sched_file)
    work_dir = "%s/tmp" % dir_name

    procs, execs = load_schedule(name, sched_file, exp_params.duration)

    exp = Experiment(name, exp_params.scheduler, work_dir, out_dir,
                     procs, execs, exp_params.tracers)

    exp.log(start_message)

    if not ignore:
        verify_environment(exp_params)

    run_script(exp_params.pre_script, exp, dir_name, work_dir)

    exp.run_exp()

    run_script(exp_params.post_script, exp, dir_name, out_dir)

    if jabber:
        jabber.send("Completed '%s'" % name)

    # Save parameters used to run experiment in out_dir
    out_params = dict(exp_params.file_params.items() +
                      [(PARAMS['sched'],  exp_params.scheduler),
                       (PARAMS['tasks'],  len(execs)),
                       (PARAMS['dur'],    exp_params.duration)])

    # Feather-trace clock frequency saved for accurate overhead parsing
    ft_freq = com.ft_freq()
    if ft_freq:
        out_params[PARAMS['cycles']] = ft_freq

    with open("%s/%s" % (out_dir, DEFAULTS['params_file']), 'w') as f:
        f.write(str(out_params))


def get_exps(opts, args):
    '''Return list of experiment files or directories'''
    if args:
        return args

    # Default to sched_file > generated dirs
    if os.path.exists(opts.sched_file):
        sys.stderr.write("Reading schedule from %s.\n" % opts.sched_file)
        return [opts.sched_file]
    elif os.path.exists(DEFAULTS['out-gen']):
        sys.stderr.write("Reading schedules from %s/*.\n" % DEFAULTS['out-gen'])
        sched_dirs = os.listdir(DEFAULTS['out-gen'])
        return ['%s/%s' % (DEFAULTS['out-gen'], d) for d in sched_dirs]
    else:
        sys.stderr.write("Run with -h to view options.\n");
        sys.exit(1)


def setup_jabber(target):
    try:
        from run.jabber import Jabber

        return Jabber(target)
    except ImportError:
        sys.stderr.write("Failed to import jabber. Is python-xmpp installed? " +
                         "Disabling instant messages.\n")
        return None


def setup_email(target):
    try:
        from run.emailer import Emailer

        return Emailer(target)
    except ImportError:
        message = "Failed to import email. Is smtplib installed?"
    except IOError:
        message = "Failed to create email. Is an smtp server active?"

    sys.stderr.write(message + " Disabling email message.\n")
    return None


def make_paths(exp, out_base_dir, opts):
    '''Translate experiment name to (schedule file, output directory) paths'''
    path = "%s/%s" % (os.getcwd(), exp)
    out_dir = "%s/%s" % (out_base_dir, os.path.split(exp.strip('/'))[1])

    if not os.path.exists(path):
        raise IOError("Invalid experiment: %s" % path)

    if opts.force and os.path.exists(out_dir):
        shutil.rmtree(out_dir)

    if os.path.isdir(path):
        sched_file = "%s/%s" % (path, opts.sched_file)
    else:
        sched_file = path

    return sched_file, out_dir


def main():
    opts, args = parse_args()

    exps = get_exps(opts, args)

    jabber = setup_jabber(opts.jabber) if opts.jabber else None
    email  = setup_email(opts.email)   if opts.email  else None

    out_base = os.path.abspath(opts.out_dir)
    created  = False
    if not os.path.exists(out_base):
        created = True
        os.mkdir(out_base)

    ran = done = succ = failed = invalid = 0

    for i, exp in enumerate(exps):
        sched_file, out_dir = make_paths(exp, out_base, opts)
        sched_dir = os.path.split(sched_file)[0]

        try:
            start_message = "Loading experiment %d of %d." % (i+1, len(exps))
            exp_params = make_exp_params(opts.scheduler, opts.duration,
                                         sched_dir, opts.param_file)

            run_experiment(exp, sched_file, exp_params, out_dir,
                           start_message, opts.ignore, jabber)

            succ += 1
        except ExperimentDone:
            sys.stderr.write("Experiment '%s' already completed " % exp +
                             "at '%s'\n" % out_base)
            done += 1
        except (InvalidKernel, InvalidConfig) as e:
            sys.stderr.write("Invalid environment for experiment '%s'\n" % exp)
            sys.stderr.write("%s\n" % e)
            invalid += 1
        except KeyboardInterrupt:
            sys.stderr.write("Keyboard interrupt, quitting\n")
            break
        except SystemCorrupted as e:
            sys.stderr.write("System is corrupted! Fix state before continuing.\n")
            sys.stderr.write("%s\n" % e)
            break
        except Exception as e:
            sys.stderr.write("Failed experiment %s\n" % exp)
            sys.stderr.write("%s\n" % e)
            failed += 1

        ran += 1

    # Clean out directory if it failed immediately
    if not os.listdir(out_base) and created and not succ:
        os.rmdir(out_base)

    message = "Experiments ran:\t%d of %d" % (ran, len(exps)) +\
      "\n  Successful:\t\t%d" % succ +\
      "\n  Failed:\t\t%d" % failed +\
      "\n  Already Done:\t\t%d" % done +\
      "\n  Invalid Environment:\t%d" % invalid

    print(message)

    if succ:
        sys.stderr.write("Successful experiment data saved in %s.\n" %
                         opts.out_dir)

    if email:
        email.send(message)
        email.close()

if __name__ == '__main__':
    main()
