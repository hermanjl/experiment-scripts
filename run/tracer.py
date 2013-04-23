import os
import config.config as conf

from common import is_device,num_cpus
from operator import methodcaller
from run.executable.ftcat import FTcat,Executable

class Tracer(object):
    def __init__(self, name, output_dir, exact=False):
        self.name = name
        self.output_dir = output_dir
        self.bins = []
        self.exact=exact

    def get_name(self):
        return self.name

    def is_exact(self):
        return self.exact

    def start_tracing(self):
        map(methodcaller("execute"), self.bins)

    def stop_tracing(self):
        map(methodcaller('terminate'), self.bins)
        map(methodcaller('wait'), self.bins)

class LinuxTracer(Tracer):
    EVENT_ROOT = "/sys/kernel/debug/tracing"
    LITMUS_EVENTS = "%s/events/litmus" % EVENT_ROOT

    def __init__(self, output_dir):
        super(LinuxTracer, self).__init__("Trace-cmd / Kernelshark", output_dir)

        extra_args = ["record", # "-e", "sched:sched_switch",
                      "-e", "litmus:*",
                      "-o", "%s/%s" % (output_dir, conf.FILES['linux_data'])]
        stdout = open('%s/trace-cmd-stdout.txt' % self.output_dir, 'w')
        stderr = open('%s/trace-cmd-stderr.txt' % self.output_dir, 'w')

        execute = Executable(conf.BINS['trace-cmd'], extra_args,
                             stdout, stderr, output_dir)
        self.bins.append(execute)

    @staticmethod
    def enabled():
        return conf.BINS['trace-cmd'] and os.path.exists(LinuxTracer.LITMUS_EVENTS)

    def stop_tracing(self):
        map(methodcaller('interrupt'), self.bins)
        map(methodcaller('wait'), self.bins)

class LogTracer(Tracer):
    DEVICE_STR = '/dev/litmus/log'

    def __init__(self, output_dir):
        super(LogTracer, self).__init__("Logger", output_dir)

        out_file = open("%s/%s" % (self.output_dir, conf.FILES['log_data']), 'w')

        cat = (Executable("/bin/cat", [LogTracer.DEVICE_STR]))
        cat.stdout_file = out_file

        self.bins.append(cat)

    @staticmethod
    def enabled():
        return is_device(LogTracer.DEVICE_STR)

    def stop_tracing(self):
        map(methodcaller('interrupt'), self.bins)
        map(methodcaller('wait', False), self.bins)

class SchedTracer(Tracer):
    DEVICE_STR = '/dev/litmus/sched_trace'

    def __init__(self, output_dir):
        super(SchedTracer, self).__init__("Sched Trace", output_dir)

        if SchedTracer.enabled():
            for cpu in range(num_cpus()):
                # Executable will close the stdout/stderr files
                stdout_f = open('%s/st-%d.bin' % (self.output_dir, cpu), 'w')
                stderr_f = open('%s/st-%d-stderr.txt' % (self.output_dir, cpu), 'w')
                dev = '{0}{1}'.format(SchedTracer.DEVICE_STR, cpu)
                ftc = FTcat(conf.BINS['ftcat'], stdout_f, stderr_f, dev,
                            conf.SCHED_EVENTS, cpu=cpu)

                self.bins.append(ftc)

    @staticmethod
    def enabled():
        return is_device("%s%d" % (SchedTracer.DEVICE_STR, 0))

class OverheadTracer(Tracer):
    DEVICE_STR = '/dev/litmus/ft_trace0'

    def __init__(self, output_dir):
        super(OverheadTracer, self).__init__("Overhead Trace", output_dir, True)

        stdout_f = open('{0}/{1}'.format(self.output_dir, conf.FILES['ft_data']), 'w')
        stderr_f = open('{0}/{1}.stderr.txt'.format(self.output_dir, conf.FILES['ft_data']), 'w')
        ftc = FTcat(conf.BINS['ftcat'], stdout_f, stderr_f,
                OverheadTracer.DEVICE_STR, conf.OVH_ALL_EVENTS)

        self.bins.append(ftc)

    @staticmethod
    def enabled():
        return is_device(OverheadTracer.DEVICE_STR)

class PerfTracer(Tracer):
    def __init__(self, output_dir):
        super(PerfTracer, self).__init__("CPU perf counters", output_dir)

    @staticmethod
    def enabled():
        return False


tracers = {}

def register_tracer(tracer, name):
    tracers[name] = tracer

def get_tracer_types(names):
    error = True # Error if name is not present
    errors = []

    if not names:
        # Just return all enabled tracers if none specified
        names = tracers.keys()
        error = False

    ret = []

    for name in names:
        if name not in tracers:
            raise ValueError("Invalid tracer '%s', valid names are: %s" %
                             (name, tracers.keys()))

        if tracers[name].enabled():
            ret += [ tracers[name] ]
        elif error:
            errors += ["Tracer '%s' requested, but not enabled." % name]

    if errors:
        raise ValueError("Check your kernel compile configuration!\n" +
                         "\n".join(errors))

    return ret

register_tracer(LinuxTracer, "kernelshark")
register_tracer(LogTracer, "log")
register_tracer(SchedTracer, "sched")
register_tracer(OverheadTracer, "overhead")

