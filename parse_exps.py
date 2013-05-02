#!/usr/bin/env python
from __future__ import print_function

import copy
import common as com
import multiprocessing
import os
import parse.ft as ft
import parse.sched as st
import pickle
import re
import shutil as sh
import sys
import traceback

from collections import namedtuple
from config.config import FILES,DEFAULTS,PARAMS
from optparse import OptionParser
from parse.point import ExpPoint
from parse.tuple_table import TupleTable
from parse.col_map import ColMapBuilder


def parse_args():
    parser = OptionParser("usage: %prog [options] [data_dir]...")

    parser.add_option('-o', '--out', dest='out',
                      help='file or directory for data output',
                      default=DEFAULTS['out-parse'])
    parser.add_option('-i', '--ignore', metavar='[PARAM...]', default="",
                      help='ignore changing parameter values')
    parser.add_option('-f', '--force', action='store_true', default=False,
                      dest='force', help='overwrite existing data')
    parser.add_option('-v', '--verbose', action='store_true', default=False,
                      dest='verbose', help='print out data points')
    parser.add_option('-m', '--write-map', action='store_true', default=False,
                      dest='write_map',
                      help='Output map of values instead of csv tree')
    parser.add_option('-p', '--processors',
                      default=max(multiprocessing.cpu_count() - 1, 1),
                      type='int', dest='processors',
                      help='number of threads for processing')
    parser.add_option('-s', '--scale-against', dest='scale_against',
                      metavar='PARAM=VALUE', default="type=unmanaged",
                      help='calculate task scaling factors against these configs')
    parser.add_option('-c', '--collapse', dest='collapse',
                      action='store_true', default=False,
                      help=('simplify graphs where possible by averaging ' +
                            'parameter values which are numbers (dangerous)'))

    return parser.parse_args()


ExpData = namedtuple('ExpData', ['path', 'params', 'work_dir'])


def parse_exp(exp_force_base):
    # Tupled for multiprocessing
    exp, force, base_exp = exp_force_base

    result_file = exp.work_dir + "/exp_point.pkl"
    should_load = not force and os.path.exists(result_file)

    result = None
    if should_load:
        with open(result_file, 'rb') as f:
            try:
                # No need to go through this work twice
                result = pickle.load(f)
            except:
                pass

    if not result:
        try:
            # Create a readable name
            name = os.path.relpath(exp.path)
            name = name if name != "." else os.path.split(os.getcwd())[1]

            result = ExpPoint(name)

            # Write overheads into result
            cycles = exp.params[PARAMS['cycles']]
            ft.extract_ft_data(result, exp.path, exp.work_dir, cycles)

            # Write scheduling statistics into result
            st.extract_sched_data(result, exp.path, exp.work_dir)

            if base_exp:
                # Write scaling factors into result
                st.extract_scaling_data(result, exp.path, base_exp.path)

            with open(result_file, 'wb') as f:
                pickle.dump(result, f)
        except:
            traceback.print_exc()

    return (exp, result)


def get_exp_params(data_dir, cm_builder):
    param_file = "%s/%s" % (data_dir, FILES['params_file'])
    if os.path.isfile(param_file):
        params = com.load_params(param_file)

        # Store parameters in cm_builder, which will track which parameters change
        # across experiments
        for key, value in params.iteritems():
            cm_builder.try_add(key, value)
    else:
         params = {}

    # Cycles must be present for feather-trace measurement parsing
    if PARAMS['cycles'] not in params:
        params[PARAMS['cycles']] = DEFAULTS['cycles']

    return params


def load_exps(exp_dirs, cm_builder, force):
    exps = []

    sys.stderr.write("Loading experiments...\n")

    for data_dir in exp_dirs:
        if not os.path.isdir(data_dir):
            raise IOError("Invalid experiment '%s'" % os.path.abspath(data_dir))

        # Used to store error output and debugging info
        work_dir = data_dir + "/tmp"

        if os.path.exists(work_dir) and force:
            sh.rmtree(work_dir)
        if not os.path.exists(work_dir):
            os.mkdir(work_dir)

        params = get_exp_params(data_dir, cm_builder)

        exps += [ ExpData(data_dir, params, work_dir) ]

    return exps


def make_base_table(cmd_scale, builder, exps):
    if not cmd_scale:
        return None

    # Configuration key for task systems used to calculate task
    # execution scaling factors
    [(param, value)] = re.findall("(.*)=(.*)", cmd_scale)


    if param not in builder:
        com.log_once("Base column '%s' not present in any parameters!" % param)
        com.log_once("Scaling factors will not be included.")

    builder.try_remove(param)
    base_map = builder.build()
    base_table = TupleTable(base_map)

    # Fill table with exps who we will scale against
    for exp in exps:
        if exp.params[param] == value:
            base_table[exp.params] = exp

    return base_table


def get_dirs(args):
    if args:
        return args
    elif os.path.exists(DEFAULTS['out-run']):
        sys.stderr.write("Reading data from %s/*\n" % DEFAULTS['out-run'])
        sched_dirs = os.listdir(DEFAULTS['out-run'])
        return ['%s/%s' % (DEFAULTS['out-run'], d) for d in sched_dirs]
    else:
        sys.stderr.write("Reading data from current directory.\n")
        return [os.getcwd()]


def get_bases(builder, exps, opts):
    '''Return a matching array of experiments against which scaling factors
    will be calculated.'''
    bases = [None]*len(exps)

    base_table = make_base_table(opts.scale_against,
                                 copy.deepcopy(builder), exps)
    if not base_table:
        return bases

    for i,exp in enumerate(exps):
        if exp.params in base_table and base_table[exp.params] != exp:
            bases[i] = base_table[exp.params]

    return bases


def fill_table(table, builder, exps, opts):
    procs  = min(len(exps), opts.processors)
    logged = multiprocessing.Manager().list()
    bases  = get_bases(builder, exps, opts)

    sys.stderr.write("Parsing data...\n")

    pool = multiprocessing.Pool(processes=procs,
    # Share a list of previously logged messages amongst processes
    # This is for the com.log_once method to use
                initializer=com.set_logged_list, initargs=(logged,))

    pool_args = zip(exps, [opts.force]*len(exps), bases)
    enum = pool.imap_unordered(parse_exp, pool_args, 1)

    try:
        for i, (exp, result) in enumerate(enum):
            if not result:
                continue

            if opts.verbose:
                print(result)
            else:
                sys.stderr.write('\r {0:.2%}'.format(float(i)/len(exps)))
                table[exp.params] += [result]

        pool.close()
    except:
        pool.terminate()
        traceback.print_exc()
        raise Exception("Failed parsing!")
    finally:
        pool.join()

    sys.stderr.write('\n')


def write_csvs(table, out, print_empty=False):
    reduced_table = table.reduce()

    # Write out csv directories for all variable params
    dir_map = reduced_table.to_dir_map()

    # No csvs to write, assume user meant to print out data
    if dir_map.is_empty():
        if print_empty:
            sys.stderr.write("Too little data to make csv files, " +
                             "printing results.\n")
            for key, exp in table:
                for e in exp:
                    print(e)
    else:
        dir_map.write(out)


def write_collapsed_csvs(table, opts):
    sys.stderr.write("Collapse option specified. "
                     "Only one numeric column at a time will be plotted.\n"
                     "The values of others will be averaged. "
                     "This is dangerous and can hide important trends!\n")

    original_map = table.get_col_map()

    builder = ColMapBuilder()
    numeric_cols = []

    # Add only nonnumeric fields to builder
    for column in original_map.columns():
        numeric = True
        for v in original_map.get_values()[column]:
            try:
                float(v)
            except ValueError:
                numeric = False
                builder.try_add(column, v)
        if numeric:
            numeric_cols += [column]

    for num_column in numeric_cols:
        # Only going to consider a single number column at a time
        for num_value in original_map.get_values()[column]:
            builder.try_add(num_column, num_value)

        next_map = builder.build()
        next_table = TupleTable(next_map)

        # Re-sort data into new table using this new key
        for mapped_key, points in table:
            kv = original_map.get_kv(mapped_key)
            next_table[kv] += points

        write_csvs(next_table, opts.out)

        builder.try_remove(num_column)


def write_output(table, opts):
    if opts.write_map:
        sys.stderr.write("Writing python map into %s...\n" % opts.out)
        reduced_table = table.reduce()
        reduced_table.write_map(opts.out)
    else:
        if opts.force and os.path.exists(opts.out):
            sh.rmtree(opts.out)

        sys.stderr.write("Writing csvs into %s...\n" % opts.out)

        if opts.collapse:
            write_collapsed_csvs(table, opts)
        else:
            write_csvs(table, opts.out, not opts.verbose)


def main():
    opts, args = parse_args()
    exp_dirs = get_dirs(args)

    # Load experiment parameters into a ColMap
    builder = ColMapBuilder()
    exps = load_exps(exp_dirs, builder, opts.force)

    # Don't track changes in ignored parameters
    if opts.ignore:
        for param in opts.ignore.split(","):
            builder.try_remove(param)

    # Always average multiple trials
    builder.try_remove(PARAMS['trial'])
    # Only need this for feather-trace parsing
    builder.try_remove(PARAMS['cycles'])

    col_map = builder.build()
    table = TupleTable(col_map)

    fill_table(table, builder, exps, opts)

    if not table:
        sys.stderr.write("Found no data to parse!")
        sys.exit(1)

    write_output(table, opts)

if __name__ == '__main__':
    main()
