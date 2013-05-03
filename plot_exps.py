#!/usr/bin/env python
from __future__ import print_function

# Without this trickery, matplotlib uses the current X windows session
# to create graphs. Problem 1 with this: requires user has an X windows,
# through ssh -X or otherws. Problem 2: it kills the performance on the
# computer running the X session, even if that computer isn't the one
# running plot_exps.py!
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plot

import common as com
import multiprocessing
import os
import shutil as sh
import sys
import traceback

from collections import namedtuple
from config.config import DEFAULTS

from optparse import OptionParser
from parse.col_map import ColMap,ColMapBuilder
from parse.dir_map import DirMap
from plot.style import make_styler

def parse_args():
    parser = OptionParser("usage: %prog [options] [csv_dir]...")

    parser.add_option('-o', '--out-dir', dest='out_dir',
                      help='directory for plot output',
                      default=DEFAULTS['out-plot'])
    parser.add_option('-f', '--force', action='store_true', default=False,
                      dest='force', help='overwrite existing data')
    parser.add_option('-p', '--processors',
                      default=max(multiprocessing.cpu_count() - 1, 1),
                      type='int', dest='processors',
                      help='number of threads for processing')

    return parser.parse_args()

ExpDetails = namedtuple('ExpDetails', ['variable', 'value', 'title',
                                       'out', 'node'])
OUT_FORMAT = 'pdf'

def get_details(node, path, out_dir):
    '''Decode a @path into details about a single experiment.'''
    out = "_".join(path) if path else "plot"
    out = "%s/%s.%s" % (out_dir, out, OUT_FORMAT)

    value = path.pop(0) if path else None
    variable = path.pop(0) if path else None

    title  = value.capitalize() if value else ""
    title += " by %s" % variable if variable else ""
    title += " (%s)" % (", ".join(path)) if path else ""

    return ExpDetails(variable, value, title, out, node)

def plot_by_variable(details):
    '''Plot each .csv files under @plot_node as a line on a shared plot.'''

    builder = ColMapBuilder()
    config_nodes = []

    # Decode file names into configuration dicts
    for line_path, line_node in details.node.children.iteritems():
        encoded = line_path[:line_path.index(".csv")]

        try:
            line_config = ColMap.decode(encoded)
        except:
            line_config = {'name': encoded}

        for k, v in line_config.iteritems():
            builder.try_add(k, v)
        config_nodes += [(line_config, line_node)]

    col_map   = builder.build()
    style_map = make_styler(col_map)

    figure = plot.figure()
    axes   = figure.add_subplot(111)

    # Create a line for each file node and its configuration
    for line_config, line_node in config_nodes:
        style  = style_map.get_style(line_config)
        values = sorted(line_node.values, key=lambda tup: tup[0])
        xvalues, yvalues = zip(*values)

        plot.plot(xvalues, yvalues, style.fmt())

    axes.set_title(details.title)

    lines, labels = zip(*style_map.get_key())
    axes.legend(tuple(lines), tuple(labels), prop={'size':10},
	    # This code places the legend slightly to the right of the plot
        bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0)

    axes.set_ylabel(details.value)
    axes.set_xlabel(details.variable)
    axes.set_xlim(0, axes.get_xlim()[1])
    axes.set_ylim(0, axes.get_ylim()[1])

    plot.savefig(details.out, format=OUT_FORMAT,
    	# Using 'tight' causes savefig to rescale the image for non-plot
	    # artists, which in our case is just the legend
        bbox_inches='tight')

    return True

def plot_wrapper(details):
    '''Wrap exceptions in named method for printing in multiprocessing pool.'''
    try:
        return plot_by_variable(details)
    except:
        traceback.print_exc()

def plot_dir(data_dir, out_dir, max_procs, force):
    sys.stderr.write("Reading data...\n")
    dir_map = DirMap.read(data_dir)

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    sys.stderr.write("Plotting...\n")

    # Count total plots for % counter
    num_plots = len([x for x in dir_map.leafs(1)])

    plot_details = []
    for plot_path, plot_node in dir_map.leafs(1):
        details = get_details(plot_node, plot_path, out_dir)

        if force or not os.path.exists(details.out):
            plot_details += [details]

    if not plot_details:
        return

    procs  = min(len(plot_details), max_procs)
    logged = multiprocessing.Manager().list()

    pool   = multiprocessing.Pool(processes=procs,
                initializer=com.set_logged_list, initargs=(logged,))

    enum  = pool.imap_unordered(plot_wrapper, plot_details)

    try:
        for i, _ in enumerate(enum):
            sys.stderr.write('\r {0:.2%}'.format(float(i)/num_plots))
        pool.close()
    except:
        pool.terminate()
        traceback.print_exc()
        raise Exception("Failed plotting!")
    finally:
        pool.join()

    sys.stderr.write('\n')

def get_dirs(args):
    if args:
        return args
    elif os.path.exists(DEFAULTS['out-parse']):
        return [DEFAULTS['out-parse']]
    else:
        return os.getcwd()

def main():
    opts, args = parse_args()
    dirs = get_dirs(args)

    if opts.force and os.path.exists(opts.out_dir):
        sh.rmtree(opts.out_dir)
    if not os.path.exists(opts.out_dir):
        os.mkdir(opts.out_dir)

    for dir in dirs:
        if len(dirs) > 1:
            out_dir = "%s/%s" % (opts.out_dir, os.path.split(dir)[1])
        else:
            out_dir = opts.out_dir
        plot_dir(dir, out_dir, opts.processors, opts.force)

    sys.stderr.write("Plots saved in %s.\n" % opts.out_dir)

if __name__ == '__main__':
    main()
