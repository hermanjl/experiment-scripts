import config.config as conf
import numpy as np
import os
import re
import shutil as sh
import sys
import subprocess

from point import Measurement,Type

FT_SPLIT_NAME  = "overhead={}.bin"
FT_SORTED_NAME = "sorted-ft.bin"
FT_ERR_NAME    = "err-ft"

def parse_overhead(result, overhead_bin, overhead, cycles, out_dir, err_file):
    '''Store statistics for @overhead in @overhead_bin into @result.'''
    ovh_fname = "{}/{}".format(out_dir, FT_SPLIT_NAME).format(overhead)

    if os.path.exists(ovh_fname):
        os.remove(ovh_fname)
    ovh_file = open(ovh_fname, 'w')

    err_file.write("Overhead is %s" % overhead)
    # Extract matching overhead events into a seperate file
    cmd  = [conf.BINS["ftsplit"], "-r", "-b", overhead, overhead_bin]
    ret  = subprocess.call(cmd, cwd=out_dir, stderr=err_file, stdout=ovh_file)
    size = os.stat(ovh_fname).st_size

    if ret:
        raise Exception("Failed (%d) with command: %s" % (ret, " ".join(cmd)))
    #if not size:
    #    os.remove(ovh_fname)
    if size and not ret:
        # Map and sort file for stats
        data = np.memmap(ovh_fname, dtype="float32", mode='c')
        data /= float(cycles) # Scale for processor speed
        data.sort()
        print("Have %s of %s" % (len(data), overhead))

        m = Measurement("%s-%s" % (overhead_bin, overhead))
        m[Type.Max] = data[-1]
        m[Type.Avg] = np.mean(data)
        m[Type.Min] = data[0]
        m[Type.Var] = np.var(data)

        result[overhead] = m

        #os.remove(ovh_fname)

def sort_ft(ft_file, err_file, out_dir):
    '''Create and return file with sorted overheads from @ft_file.'''
    out_fname = "{}/{}".format(out_dir, FT_SORTED_NAME)

    # Sort happens in-place
    sh.copyfile(ft_file, out_fname)
    cmd = [conf.BINS['ftsort'], out_fname]

    ret = subprocess.call(cmd, cwd=out_dir, stderr=err_file, stdout=err_file)
    if ret:
        raise Exception("Sort failed (%d) with command: %s" % (ret, " ".join(cmd)))

    return out_fname

def extract_ft_data(result, data_dir, work_dir, cycles):
    data_dir = os.path.abspath(data_dir)
    work_dir = os.path.abspath(work_dir)

    freg = conf.FILES['ft_matches'] + "$"
    bins = [f for f in os.listdir(data_dir) if re.match(freg, f)]

    if not len(bins):
        return False

    bin_file = "{}/{}".format(data_dir, bins[0])
    if not os.path.getsize(bin_file):
        return False

    with open("%s/%s" % (work_dir, FT_ERR_NAME), 'w') as err_file:
        sorted_bin = sort_ft(bin_file, err_file, work_dir)

        for event in conf.OVH_BASE_EVENTS:
            print event
            parse_overhead(result, sorted_bin, event, cycles,
                           work_dir, err_file)

        #os.remove(sorted_bin)

    return True
