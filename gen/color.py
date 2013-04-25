import os
import re

from collections import namedtuple,defaultdict
from math import ceil
from random import randint

class ColorScheme(object):
    def __init__(self, colors, ways):
        self.colors = colors
        self.ways = ways

    def color(self, tasks, wss):
        '''Assign a color->replicas dict to each task in tasks.'''
        raise NotImplementedError

class BlockColorScheme(ColorScheme):
    def __init__(self, colors, ways, way_first):
        super(BlockColorScheme, self).__init__(colors, ways)
        self.way_first = way_first

    def color(self, tasks, pages_needed):
        '''Pages are assigned in blocks, either maximizing the number of ways
        or maximizing the number of colors used.'''
        cpus = defaultdict(list)
        for t in tasks:
            cpus[t.cpu].append(t)

        if self.way_first:
            # Way first means maximize ways
            pages_per_color = min(self.ways, pages_needed)
            colors_per_task = int(ceil(float(pages_needed)/pages_per_color))
        else:
            # Color first means maximize colors
            colors_per_task = min(self.colors, pages_needed)
            pages_per_color = int(ceil(float(pages_needed)/colors_per_task))

        curr_color = 0
        for cpu, tasks in cpus.iteritems():
            # All tasks on a CPU have the same coloring scheme
            cpu_colors = defaultdict(int)
            for _ in xrange(colors_per_task):
                cpu_colors[curr_color] = pages_per_color
                curr_color = (curr_color + 1) % self.colors

            if sum(cpu_colors.values()) < pages_needed:
                raise Exception("Failed to block color cpu, %s" % cpu_colors)

            for t in tasks:
                t.colors = cpu_colors

class RandomColorScheme(ColorScheme):
    def color(self, tasks, pages_needed):
        '''Pages are placed randomly in the cache'''
        if pages_needed >= self.ways * self.colors:
            raise Exception("Too many pages: %d > %d * %d" %
                            (pages_needed, self.ways, self.colors))

        for t in tasks:
            t.colors = defaultdict(int)

            for _ in xrange(pages_needed):
                # Find the next color with available ways
                while True:
                    next_color = randint(0, self.colors - 1)
                    if t.colors[next_color] != self.ways:
                        break

                t.colors[next_color] += 1;

class EvilColorScheme(ColorScheme):
    def color(self, tasks, pages_needed):
        '''All tasks' working sets are placed at the front of the cache'''
        colors = defaultdict(int)
        color  = 0

        while pages_needed > 0:
            colors[color] = min(self.ways, pages_needed)
            pages_needed -= colors[color]

            color += 1

        for t in tasks:
            t.colors = colors

INFO_FIELDS = ['cache', 'line', 'page', 'ways', 'sets', 'colors']
INFO_PROC   = '/proc/sys/litmus/color/cache_info'

# Build parsing regex
FIELD_REGEX = r"(?:.*?{0}.*?(?P<{0}>\d+).*?)"
INFO_REGEX  = "|".join([FIELD_REGEX.format(field) for field in INFO_FIELDS])
INFO_REGEX  = r"(?:{})*".format(INFO_REGEX)

# To fill up this
CacheInfo = namedtuple('CacheInfo', INFO_FIELDS)

def get_cache_info():
    if os.path.exists(INFO_PROC):
        with open(INFO_PROC, 'r') as f:
            data   = f.read()
            values = re.search(INFO_REGEX, data, re.M|re.I|re.S).groupdict()
            return CacheInfo(**values)
    else:
        return None
