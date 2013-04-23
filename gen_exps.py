#!/usr/bin/env python
from __future__ import print_function

import gen.generator as gen
import os
import re
import shutil as sh
import sys

from config.config import DEFAULTS
from optparse import OptionParser

def parse_args():
    parser = OptionParser("usage: %prog [options] [files...] "
                          "[generators...] [param=val[,val]...]")

    parser.add_option('-o', '--out-dir', dest='out_dir',
                      help='directory for data output',
                      default=("%s/%s"% (os.getcwd(), DEFAULTS['out-gen'])))
    parser.add_option('-f', '--force', action='store_true', default=False,
                      dest='force', help='overwrite existing data')
    parser.add_option('-n', '--num-trials', default=1, type='int', dest='trials',
                      help='number of task systems for every config')
    parser.add_option('-l', '--list-generators', dest='list_gens',
                      help='list allowed generators', action='store_true',
                      default=False)
    parser.add_option('-d', '--describe-generators', metavar='generator[,..]',
                      dest='described', default=None,
                      help='describe parameters for generator(s)')

    return parser.parse_args()

def load_file(fname):
    with open(fname, 'r') as f:
        data = f.read().strip()
    try:
        values = eval(data)
        if 'generator' not in values:
            raise ValueError()
        generator = values['generator']
        del values['generator']
        return generator, values
    except:
           raise IOError("Invalid generation file: %s" % fname)

def main():
    opts, args = parse_args()

    # Print generator information on the command line
    if opts.list_gens:
        print(", ".join(gen.get_generators()))
    if opts.described != None:
        for generator in opts.described.split(','):
            if generator not in gen.get_generators():
                sys.stderr.write("No generator '%s'\n" % generator)
            else:
                print("Generator '%s', " % generator)
                gen.get_generators()[generator]().print_help()
    if opts.list_gens or opts.described:
        return 0

    params = filter(lambda x : re.match("\w+=\w+", x), args)

    # Ensure some generator is loaded
    args = list(set(args) - set(params))
    args = args or gen.get_generators().keys()

    # Split into files to load and named generators
    files = filter(os.path.exists, args)
    gen_list = list(set(args) - set(files))

    # Parse all specified parameters to be applied to every experiment
    global_params = dict(map(lambda x : tuple(x.split("=")), params))
    for k, v in global_params.iteritems():
        global_params[k] = v.split(',')

    exp_sets  = map(load_file, files)
    exp_sets += map(lambda x: (x, {}), gen_list)

    if opts.force and os.path.exists(opts.out_dir):
        sh.rmtree(opts.out_dir)
    if not os.path.exists(opts.out_dir):
        os.mkdir(opts.out_dir)

    for gen_name, gen_params in exp_sets:
        if gen_name not in gen.get_generators():
            raise ValueError("Invalid generator '%s'" % gen_name)

        sys.stderr.write("Creating experiments with %s generator...\n" % gen_name)

        params = dict(gen_params.items() + global_params.items())
        clazz  = gen.get_generators()[gen_name]

        generator = clazz(params=params)

        generator.create_exps(opts.out_dir, opts.force, opts.trials)

    sys.stderr.write("Experiments saved in %s.\n" % opts.out_dir)

if __name__ == '__main__':
    main()
