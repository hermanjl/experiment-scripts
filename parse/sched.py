import config.config as conf
import os
import re
import struct
import subprocess
import sys

from collections import defaultdict,namedtuple
from common import recordtype
from point import Measurement

class TimeTracker:
    '''Store stats for durations of time demarcated by sched_trace records.'''
    def __init__(self):
        self.begin = self.avg = self.max = self.num = self.job = 0

    def store_time(self, record):
        '''End duration of time.'''
        dur = record.when - self.begin

        if self.job == record.job and dur > 0:
            self.max  = max(self.max, dur)
            self.avg *= float(self.num / (self.num + 1))
            self.num += 1
            self.avg += dur / float(self.num)

            self.begin = 0
            self.job   = 0

    def start_time(self, record):
        '''Start duration of time.'''
        self.begin = record.when
        self.job   = record.job

# Data stored for each task
TaskParams = namedtuple('TaskParams',  ['wcet', 'period', 'cpu', 'level'])
TaskData   = recordtype('TaskData',    ['params', 'jobs', 'loads',
                                        'blocks', 'misses', 'execs'])

# Map of event ids to corresponding class, binary format, and processing methods
RecordInfo = namedtuple('RecordInfo', ['clazz', 'fmt', 'method'])
record_map = [0]*10

# Common to all records
HEADER_FORMAT = '<bbhi'
HEADER_FIELDS = ['type', 'cpu', 'pid', 'job']
RECORD_SIZE   = 24

NSEC_PER_MSEC = 1000000

def register_record(name, id, method, fmt, fields):
    '''Create record description from @fmt and @fields and map to @id, using
    @method to process parsed record.'''
    # Format of binary data (see python struct documentation)
    rec_fmt = HEADER_FORMAT + fmt

    # Corresponding field data
    rec_fields = HEADER_FIELDS + fields
    if "when" not in rec_fields: # Force a "when" field for everything
        rec_fields += ["when"]

    # Create mutable class with the given fields
    field_class = recordtype(name, list(rec_fields))
    clazz = type(name, (field_class, object), {})

    record_map[id] = RecordInfo(clazz, rec_fmt, method)

def make_iterator(fname):
    '''Iterate over (parsed record, processing method) in a
    sched-trace file.'''
    if not os.path.getsize(fname):
        # Likely a release master CPU
        return

    f = open(fname, 'rb')
    max_type = len(record_map)

    while True:
        data = f.read(RECORD_SIZE)

        try:
            type_num = struct.unpack_from('b',data)[0]
        except struct.error:
            break

        rdata = record_map[type_num] if type_num <= max_type else 0
        if not rdata:
            continue

        try:
            values = struct.unpack_from(rdata.fmt, data)
        except struct.error:
            continue

        obj = rdata.clazz(*values)
        yield (obj, rdata.method)

def read_data(task_dict, fnames):
    '''Read records from @fnames and store per-pid stats in @task_dict.'''
    buff = []

    def add_record(itera):
        # Ordered insertion into buff
        try:
            next_ret = itera.next()
        except StopIteration:
            return

        arecord, method = next_ret
        i = 0
        for (i, (brecord, m, t)) in enumerate(buff):
            if brecord.when > arecord.when:
                break
        buff.insert(i, (arecord, method, itera))

    for fname in fnames:
        itera = make_iterator(fname)
        add_record(itera)

    while buff:
        (record, method, itera) = buff.pop(0)

        add_record(itera)
        method(task_dict, record)

def process_completion(task_dict, record):
    task_dict[record.pid].misses.store_time(record)
    task_dict[record.pid].loads += [record.load]

def process_release(task_dict, record):
    data = task_dict[record.pid]
    data.jobs += 1
    data.misses.start_time(record)

def process_param(task_dict, record):
    level = chr(97 + record.level)
    params = TaskParams(record.wcet, record.period,
                        record.partition, level)
    task_dict[record.pid].params = params

def process_block(task_dict, record):
    task_dict[record.pid].blocks.start_time(record)

def process_resume(task_dict, record):
    task_dict[record.pid].blocks.store_time(record)

def process_switch_to(task_dict, record):
    task_dict[record.pid].execs.start_time(record)

def process_switch_away(task_dict, record):
    task_dict[record.pid].execs.store_time(record)

register_record('ResumeRecord', 9, process_resume, 'Q8x', ['when'])
register_record('BlockRecord', 8, process_block, 'Q8x', ['when'])
register_record('CompletionRecord', 7, process_completion, 'QQ', ['when', 'load'])
register_record('ReleaseRecord', 3, process_release, 'QQ', ['release', 'when'])
register_record('SwitchToRecord', 5, process_switch_to, 'Q8x', ['when'])
register_record('SwitchAwayRecord', 6, process_switch_away, 'Q8x', ['when'])
register_record('ParamRecord', 2, process_param, 'IIIcccx',
                ['wcet','period','phase','partition', 'task_class', 'level'])

saved_stats = []
def get_task_data(data_dir, work_dir = None):
    '''Parse sched trace files'''
    if data_dir in saved_stats:
        return data_dir

    bin_files   = conf.FILES['sched_data'].format(".*")
    output_file = "%s/out-st" % work_dir

    bins = ["%s/%s" % (data_dir,f) for f in os.listdir(data_dir) if re.match(bin_files, f)]
    if not len(bins):
        return

    # Save an in-english version of the data for debugging
    # This is optional and will only be done if 'st_show' is in PATH
    if work_dir and conf.BINS['st_show']:
        cmd_arr = [conf.BINS['st_show']]
        cmd_arr.extend(bins)
        with open(output_file, "w") as f:
            print("calling %s" % cmd_arr)
            subprocess.call(cmd_arr, cwd=data_dir, stdout=f)

    task_dict = defaultdict(lambda :TaskData(0, 0, 0, [], TimeTracker(),
                                             TimeTracker(), TimeTracker()))

    # Gather per-task values
    read_data(task_dict, bins)

    saved_stats[data_dir] = task_dict
    return task_dict

class LeveledArray(object):
    """Groups statistics by the level of the task to which they apply"""
    def __init__(self):
        self.name = name
	    self.vals = defaultdict(lambda: defaultdict(lambda:[]))

    def add(self, name, level, value):
        if type(value) != type([]):
            value = [value]
        self.vals[name][task.config.level] += value

    def write_measurements(self, result):
        for stat_name, stat_data in self.vals.iteritems():
            for level, values in stat_data.iteritems():
                if not values:
                    continue

                name = "%s%s" % ("%s-" % level if level else "", stat_name)
                result[name] = Measurement(name).from_array(arr)

def extract_sched_data(result, data_dir, work_dir):
    task_dict = get_task_data(data_dir, work_dir)

    stat_data = LeveledArray()
    for tdata in task_dict.itervalues():
        if not tdata.params:
            # Currently unknown where these invalid tasks come from...
            continue

        miss_ratio = float(tdata.misses.num) / tdata.jobs
        # Scale average down to account for jobs with 0 tardiness
        avg_tard = tdata.misses.avg * miss_ratio

        level = tdata.params.level
        stat_data.add("miss-ratio", level, miss_ratio)
        stat_data.add("avg-tard",   level, avg_tard / tdata.params.wcet)
        stat_data.add("max-tard",   level, tdata.misses.max / tdata.params.wcet)
        stat_data.add("avg-block",  level, tdata.blocks.avg / NSEC_PER_MSEC)
        stat_data.add("max-block",  level, tdata.blocks.max / NSEC_PER_MSEC)

    stat_data.write_measurements(result)

ScaleData = namedtuple('ScaleData', ['reg_tasks', 'base_tasks'])
def extract_mc_data(result, data_dir, base_dir):
    task_dict = get_task_data(data_dir)
    base_dict = get_task_data(base_dir)

    stat_data = LeveledArray()

    # Only level B loads are measured
    for tdata in filter(task_dict.iteritems(), lambda x: x.level == 'b'):
        stat_data.add('load', tdata.config.level, tdata.loads)

    tasks_by_config = defaultdict(lambda: ScaleData([], []))

    # Add tasks in order of pid to tasks_by_config
    # Tasks must be ordered by pid or we can't make 1 to 1 comparisons
    # when multiple tasks have the same config in each task set
    for tasks, field in ((task_dict, 'reg_tasks'), (base_dict, 'base_tasks')):
        for pid in sorted(tasks.keys()):
            tdata = tasks[pid]
        tlist  = getattr(tasks_by_config[tdata.params], field)
        tlist += [tdata.execs]

    # Write scaling factors
    for config, scale_data in tasks_by_config:
        if len(scale_data.reg_tasks) != len(scale_data.base_tasks):
            # Can't make comparison if different numbers of tasks!
            continue

        all_pairs = zip(scale_data.reg_tasks, scale_data.base_tasks)
        for reg_execs, base_execs in all_pairs:
            if not reg_execs.max  or not reg_execs.avg or\
               not base_execs.max or not base_execs.avg:
                # This was an issue at some point, not sure if it still is
                continue

            max_scale = float(base_execs.max) / reg_execs.max
            avg_scale = float(base_execs.avg) / reg_execs.avg

            if (avg_scale < 1 or max_scale < 1) and config.level == "b":
                sys.stderr.write("Task in {} with config {} has <1.0 scale!"
                                 .format(data_dir, config)
                continue

            stat_data.add('max-scale', config.level, max_scale)
            stat_data.add('avg-scale', config.level, avg_scale)

    stat_data.write_measurements(result)
