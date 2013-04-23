from __future__ import print_function
import itertools
from common import get_executable_hint,ft_freq

'''Paths to binaries.'''
BINS = {'rtspin'    : get_executable_hint('rtspin', 'liblitmus'),
        'release'   : get_executable_hint('release_ts', 'liblitmus'),
        'ftcat'     : get_executable_hint('ftcat', 'feather-trace-tools'),
        'ftsplit'   : get_executable_hint('ft2csv', 'feather-trace-tools'),
        'ftsort'    : get_executable_hint('ftsort', 'feather-trace-tools'),
        'st_trace'  : get_executable_hint('st_trace', 'feather-trace-tools'),
        # Optional, as not everyone uses kernelshark yet
        'trace-cmd' : get_executable_hint('trace-cmd', 'rt-kernelshark', True),
        # Optional, as sched_trace is not a publically supported repository
        'st_show'   : get_executable_hint('st_show', 'sched_trace', True)}

'''Names of output files.'''
FILES = {'ft_data'    : 'ft.bin',
         'ft_matches' : r'(ft.*\.bin$)|(.*\.ft)',
         'linux_data' : 'trace.dat',
         'sched_data' : 'st-{}.bin',
         'log_data'   : 'trace.slog'}

'''Default parameter names in params.py.'''
PARAMS = {'sched'   : 'scheduler',       # Scheduler used by run_exps
          'dur'     : 'duration',        # Duration of tests in run_exps
          'kernel'  : 'uname',           # Regex of required OS name in run_exps
          'copts'   : 'config-options',  # Required kernel configuration options
          'cycles'  : 'clock-frequency', # Frequency run_exps was run with
          'tasks'   : 'tasks',           # Number of tasks
          'trial'   : 'trial',           # For multiple exps with same config
          'pre'     : 'pre-experiment',  # Run before each experiment
          'post'    : 'post-experiment', # Run after each experiment
          'trace'   : 'tracers'          # Tracers to run with an experiment
          }

'''Default values for program options.'''
DEFAULTS = {'params_file' : 'params.py',
            'sched_file'  : 'sched.py',
            'duration'    : 10,
            'prog'        : 'rtspin',
            'out-gen'     : 'exps',
            'out-run'     : 'run-data',
            'out-parse'   : 'parse-data',
            'out-plot'    : 'plot-data',
            'cycles'      : ft_freq() or 2000}


'''Default sched_trace events (this is all of them).'''
SCHED_EVENTS = range(501, 513)

'''Overhead events.'''
OVH_BASE_EVENTS  = ['SCHED', 'RELEASE', 'SCHED2', 'TICK', 'CXS', 'LOCK', 'UNLOCK']
OVH_ALL_EVENTS   = ["%s_%s" % (e, t) for (e,t) in
                    itertools.product(OVH_BASE_EVENTS, ["START","END"])]
OVH_ALL_EVENTS  += ['RELEASE_LATENCY']
# This event doesn't have a START and END
OVH_BASE_EVENTS += ['RELEASE_LATENCY']

# If a task is missing more than this many records, its measurements
# are not included in sched_trace summaries
MAX_RECORD_LOSS = .2
