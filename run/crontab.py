from __future__ import print_function

import common
import os
import re
import sys

from subprocess import Popen, PIPE, check_output

PANIC_DUR = 10
DELAY = 30
DELAY_INTERVAL = 10

def get_cron_data():
    try:
        return check_output(['crontab', '-l'])
    except:
        return ""

def wall(message):
    '''A wall command with no header'''
    return "echo '%s' | wall -n" % message

def sanitize(args, ignored):
    ret_args = []
    for a in args:
        if a in ignored:
            continue
        if '-' == a[0] and '--' != a[0:2]:
            for i in ignored:
                a = a.replace(i, '')
        ret_args += [a]
    return ret_args

def get_outfname():
    return "cron-%s.txt" % common.get_cmd()

def get_boot_cron(ignored_params, extra=""):
    '''Turn current python script into a crontab reboot entry'''
    job_args = sanitize(sys.argv, ignored_params)
    job = " ".join(job_args)
    out_fname = get_outfname()

    short_job = " ".join([common.get_cmd()] + job_args[1:])
    msg = "Job '%s' will write output to '%s'" % (short_job, out_fname)

    sys.stderr.write("%s %d seconds after reboot.\n" % (msg, DELAY))

    # Create sleep and wall commands which will countdown DELAY seconds
    # before executing the job
    cmds = ["sleep %d" % DELAY_INTERVAL]
    delay_rem = DELAY - DELAY_INTERVAL
    while delay_rem > 0:
        wmsg = "Restarting experiments in %d seconds. %s" % (delay_rem, extra)
        cmds += [wall(wmsg)]
        cmds += ["sleep %d" % min(DELAY_INTERVAL, delay_rem)]
        delay_rem -= DELAY_INTERVAL
    delay_cmd = ";".join(cmds)

    # Create command which will only execute if the same kernel is running
    kern = common.kernel()
    fail_wall = wall("Need matching kernel '%s' to run!" % kern)
    run_cmd = "echo '%s' | grep -q `uname -r` && %s && %s && %s >> %s 2>>%s || %s" %\
      (kern, wall(msg), wall("Starting..."), job, out_fname, out_fname, fail_wall)

    return "@reboot cd %s; %s; %s;" % (os.getcwd(), delay_cmd, run_cmd)

def set_panic_restart(bool_val):
    '''Enable / disable restart on panics'''
    if bool_val:
        sys.stderr.write("Kernel will reboot after panic.\n")
        dur = PANIC_DUR
    else:
        sys.stderr.write("Kernel will no longer reboot after panic.\n")
        dur = 0

    check_output(['sysctl', '-w', "kernel.panic=%d" % dur,
                  "kernel.panic_on_oops=%d" % dur])

def write_cron_data(data):
    '''Write new crontab entry. No blank lines are written'''

    # I don't know why "^\s*$" doesn't match, hence this ugly regex
    data = re.sub(r"\n\s*\n", "\n", data, re.M)

    sp = Popen(["crontab", "-"], stdin=PIPE)
    stdout, stderr = sp.communicate(input=data)

def install_path():
    '''Place the current path in the crontab entry'''
    data = get_cron_data()
    curr_line = re.findall(r"PATH=.*", data)

    if curr_line:
        curr_paths = re.findall(r"((?:\/\w+)+)", curr_line[0])
        data = re.sub(curr_line[0], "", data)
    else:
        curr_paths = []
    curr_paths = set(curr_paths)

    for path in os.environ["PATH"].split(os.pathsep):
        curr_paths.add(path)

    data = "PATH=" + os.pathsep.join(curr_paths) + "\n" + data

    write_cron_data(data)

def install_boot_job(ignored_params, reboot_message):
    '''Re-run the current python script on system reboot using crontab'''
    remove_boot_job()

    data = get_cron_data()
    job  = get_boot_cron(ignored_params, reboot_message)

    set_panic_restart(True)

    write_cron_data(data + job + "\n")

    if job not in get_cron_data():
        raise IOError("Failed to write %s into cron!" % job)
    else:
        install_path()

def clean_output():
    fname = get_outfname()
    if os.path.exists(fname):
        os.remove(fname)

def kill_boot_job():
    remove_boot_job()

    cmd = common.get_cmd()

    procs = check_output("ps -eo pid,args".split(" "))
    pairs = re.findall("(\d+) (.*)", procs)

    for pid, args in pairs:
        if re.search(r"/bin/sh -c.*%s"%cmd, args):
            sys.stderr.write("Killing job %s\n" % pid)
            check_output(("kill -9 %s" % pid).split(" "))

def remove_boot_job():
    '''Remove installed reboot job from crontab'''
    data  = get_cron_data()
    regex = re.compile(r".*%s.*" % re.escape(common.get_cmd()), re.M)

    if regex.search(data):
        new_cron = regex.sub("", data)
        write_cron_data(new_cron)

        set_panic_restart(False)
