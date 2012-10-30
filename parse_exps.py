#!/usr/bin/env python
from __future__ import print_function

import config.config as conf
import copy
import os
import parse.ft as ft
import parse.sched as st
import re
import shutil as sh

from collections import namedtuple
from common import load_params
from optparse import OptionParser
from parse.point import ExpPoint
from parse.tuple_table import ColMap,TupleTable

def parse_args():
    # TODO: convert data-dir to proper option
    parser = OptionParser("usage: %prog [options] [data_dir]...")

    parser.add_option('-o', '--out', dest='out',
                      help='file or directory for data output', default='parse-data')

    # TODO: this means nothing
    parser.add_option('-c', '--clean', action='store_true', default=False,
                      dest='clean', help='do not output single-point csvs')
    parser.add_option('-s', '--scale-against', dest='scale_against',
                      metavar='PARAM=VALUE', default="",
                      help='calculate task scaling factors against these configs')
    parser.add_option('-f', '--force', action='store_true', default=False,
                      dest='force', help='overwrite existing data')
    parser.add_option('-v', '--verbose', action='store_true', default=False,
                      dest='verbose', help='print out data points')
    parser.add_option('-m', '--write-map', action='store_true', default=False,
                      dest='write_map',
                      help='Output map of values instead of csv tree')

    return parser.parse_args()

ExpData   = namedtuple('ExpData', ['name', 'params', 'data_files'])
DataFiles = namedtuple('DataFiles', ['ft','st'])

def get_exp_params(data_dir, col_map):
    param_file = "%s/%s" % (data_dir, conf.DEFAULTS['params_file'])
    if not os.path.isfile:
        raise Exception("No param file '%s' exists!" % param_file)

    # Keep only params that uniquely identify the experiment
    params = load_params(param_file)
    for ignored in conf.PARAMS.itervalues():
        if ignored in params:
            params.pop(ignored)

    # Track all changed params
    for key, value in params.iteritems():
        col_map.try_add(key, value)

    return params


def gen_exp_data(exp_dirs, base_conf, col_map, force):
    plain_exps = []
    scaling_bases  = []

    for data_dir in exp_dirs:
        if not os.path.isdir(data_dir):
            raise IOError("Invalid experiment '%s'" % os.path.abspath(data_dir))

        tmp_dir = data_dir + "/tmp"
        if not os.path.exists(tmp_dir):
            os.mkdir(tmp_dir)

        # Read and translate exp output files
        params = get_exp_params(data_dir, col_map)
        st_output = st.get_st_output(data_dir, tmp_dir, force)
        ft_output = ft.get_ft_output(data_dir, tmp_dir, force)

        # Create experiment named after the data dir
        exp_data = ExpData(data_dir, params, DataFiles(ft_output, st_output))

        if base_conf and base_conf.viewitems() & params.viewitems():
            if not st_output:
                raise Exception("Scaling base '%s' useless without sched data!"
                                % data_dir)
            params.pop(base_conf.keys()[0])
            scaling_bases += [exp_data]
        else:
            plain_exps += [exp_data]

    return (plain_exps, scaling_bases)

def main():
    opts, args = parse_args()

    args = args or [os.getcwd()]

    # Configuration key for task systems used to calculate task
    # execution scaling factors
    base_conf = dict(re.findall("(.*)=(.*)", opts.scale_against))

    col_map = ColMap()

    (plain_exps, scaling_bases) = gen_exp_data(args, base_conf, col_map, opts.force)

    if base_conf and base_conf.keys()[0] not in col_map:
        raise IOError("Base column '%s' not present in any parameters!" %
                      base_conf.keys()[0])

    base_table = TupleTable(col_map) # For tracking 'base' experiments
    result_table  = TupleTable(col_map) # For generating csv directories

    # Used to find matching scaling_base for each experiment
    for base in scaling_bases:
        base_table.add_exp(base.params, base)

    for exp in plain_exps:
        result = ExpPoint(exp.name)

        if exp.data_files.ft:
            # Write overheads into result
            ft.extract_ft_data(exp.data_files.ft, result, conf.BASE_EVENTS)

        if exp.data_files.st:
            base = None
            if base_conf:
                # Try to find a scaling base
                base_params = copy.deepcopy(exp.params)
                base_params.pop(base_conf.keys()[0])
                base = base_table.get_exps(base_params)[0]

            # Write deadline misses / tardiness into result
            st.extract_sched_data(exp.data_files.st, result,
                                  base.data_files.st if base else None)

        result_table.add_exp(exp.params, result)

        if opts.verbose:
            print(result)

    if opts.force and os.path.exists(opts.out):
        sh.rmtree(opts.out)

    result_table.reduce()

    if opts.write_map:
        # Write summarized results into map
        result_table.write_map(opts.out)
    else:
        # Write out csv directories for all variable params
        result_table.write_csvs(opts.out)

if __name__ == '__main__':
    main()
