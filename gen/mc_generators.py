import gen.rv as rv

from color import get_cache_info,CacheInfo,BlockColorScheme,RandomColorScheme,EvilColorScheme
from common import try_get_config_option, log_once
from gen.generator import GenOption,Generator,NAMED_UTILIZATIONS,NAMED_PERIODS
from parse.col_map import ColMap
from parse.tuple_table import TupleTable


NAMED_SHARES = {
    'weighted' : [1, 1, 8, 1],
    'harmonic' : [1, 2, 4, 1],
    'fair'     : [1, 1, 1, 1],
}

TP_BASE = """#for $t in $lvl{0}
{1}-g {0} -s $scale{0} $t.cost $t.period
#end for"""
TP_LVLA = """#if $lvla
/proc/litmus/plugins/MC-CE/ce_file{
#for $t in $lvla
$t.cpu, $t.lvla_id, $t.budget
#end for
}
#end if
""" + TP_BASE.format("a", "-i $t.lvla_id -p $t.cpu ")
TP_LVLB = TP_BASE.format("b", "-p $t.cpu ")
TP_LVLC = TP_BASE.format("c", "")
TP_LVLD = """#if $be
#for $i in range($cpus)
#if $d_fifo
#set $fopt='-f'
#else
#set $fopt=''
#end if
#if d_nice
#set $nopt='-n'
#else
#set $nopt=''
#end if
bespin -s $i -f $i.misses $be_opts -p $i $fopt $nopt
#end for
#end if"""

TM_OPT = 'MERGE_TIMERS'
SS_OPT = 'PLUGIN_MC_LINUX_SLACK_STEALING'
RD_OPT = 'PLUGIN_MC_REDIRECT'
MC_OPT = 'PLUGIN_MC'
LEVELS = 3

class McGenerator(Generator):
    def __init__(self, name="MC",
                 templates=[TP_LVLA, TP_LVLB, TP_LVLC, TP_LVLD],
                 options=[], params={}):
        super(McGenerator, self).__init__(name,
                                          templates,
                                          self.__make_options() + options,
                                          params)

    def __make_options(self):
        timer_merging = try_get_config_option(TM_OPT, False)
        slack_stealing = try_get_config_option(SS_OPT, False)
        redirect = try_get_config_option(RD_OPT, True)

        return [GenOption('levels', range(1, LEVELS+1), [3],
                          'Number of criticality levels: C, BC, ABC'),
                GenOption('be', [True,False], [False],
                          'Execute background work using bespin.'),

                GenOption('timer_merging', [True,False], [timer_merging],
                          'Require timer-merging support.'),
                GenOption('redirect', [True,False], [redirect],
                           'Redirect work to the interrupt master.'),
                GenOption('slack_stealing', [True,False], [slack_stealing],
                           'Schedule linux tasks in the background.'),

                GenOption('d_nice', [True,False], [False],
                           'Schedule level-D tasks using nice().'),
                GenOption('d_fifo', [True, False], [False],
                          'Schedule level-D tasks under SCHED_FIFO.'),

                GenOption('a_hyperperiod', float, 25.0,
                           'Level-A hyperperiod (ms).'),
                Generator._dist_option('b_periods', ['harmonic'], NAMED_PERIODS,
                'Level-B task periods. Harmonic is always used if level A is present.'),
                Generator._dist_option('c_periods', ['harmonic'],
                                       NAMED_PERIODS, 'Level-C task periods.'),

                Generator._dist_option('a_utils', ['bimo-light'],
                                       NAMED_UTILIZATIONS,
                                       'Level-A task utilizations (at level A).'),
                Generator._dist_option('b_utils', ['bimo-light'],
                                       NAMED_UTILIZATIONS,
                                       'Level-B task utilizations (at level B).'),
                Generator._dist_option('c_utils', ['bimo-light'],
                                       NAMED_UTILIZATIONS,
                                       'Level-C task utilizations (at level C).'),

                Generator._dist_option('shares', ['fair'], NAMED_SHARES,
                                       'Distribution of runtime utilizations.')]

    def __partition_worst_fit(self, params, ts):
        cpus = int(params['cpus'])
        if params['release_master']:
            # No level B on the release master
            cpus -= 1

        # Partition using worst-fit for most even distribution
        utils = [0]*cpus
        tasks = [0]*cpus
        for t in ts:
            t.cpu = utils.index(min(utils))
            t.lvla_id = tasks[t.cpu]

            utils[t.cpu] += t.utilization()
            tasks[t.cpu] += 1

            # Increment by one so release master has no tasks
            t.cpu += 1

    def __adjust(self, params, level):
        # Adjust for levels which aren't used
        num = params['levels']
        shares = list(self.shares)
        if num < 4:
            shares.pop()
        if num < 3:
            shares.pop(0)
            level -= 1

        return shares, level

    def __get_max_util(self, params, level):
        shares, level = self.__adjust(params, level)
        return float(shares[level]) / sum(shares[:level+1]) * params['cpus']

    def __get_scale(self, params, level):
        shares, level = self.__adjust(params, level)
        return float(sum(shares[:level+1])) / sum(shares)

    def __create_lvla_sched(self, params):
        if params['levels'] < 3:
            return []

        utils = self._create_dist('utilization', params['a_utils'],
                                  NAMED_UTILIZATIONS)
        periods = self._create_dist('period', params['a_hyperperiod'], None)

        ts = self._create_taskset(params, periods, utils)

        # Make the budget used by the cyclic executive larger than the
        # actual WCET because of overheads
        for t in ts:
            t.budget = int(1.05 * 1000000 * t.cost)
            t.wcet = int(t.cost)

        self.__partition_worst_fit(params, ts)

        return ts

    def __create_lvlb_sched(self, params):
        if params['levels'] < 2:
            return []

        utils = self._create_dist('utilization', params['b_utils'],
                                  NAMED_UTILIZATIONS)


        if params['levels'] > 2:
            # Level-A is present, b must be harmonic with lvla hyperperiod
            plist = [params['a_hyperperiod']*2**x for x in xrange(0, 4)]
            periods = rv.uniform_choice(plist)
        else:
            # Level b can have whatever periods it wants
            periods = self._create_dist('period', params['b_periods'],
                                        NAMED_PERIODS)
        max_util = self.__get_max_util(params, 1)

        ts = self._create_taskset(params, periods, utils, max_util)
        self.__partition_worst_fit(params, ts)

        return ts

    def __create_lvlc_sched(self, params):
        utils = self._create_dist('utilization', params['c_utils'],
                                  NAMED_UTILIZATIONS)
        periods = self._create_dist('period', params['c_periods'],
                                    NAMED_PERIODS)
        max_util = self.__get_max_util(params, 2)

        return self._create_taskset(params, periods, utils, max_util)

    def _get_tasks(self, params):
        return {'lvla': self.__create_lvla_sched(params),
                'lvlb': self.__create_lvlb_sched(params),
                'lvlc': self.__create_lvlc_sched(params)}

    def _create_exp(self, params):
        # Ugly way of doing it
        self.shares = self._create_dist('shares', params['shares'],
                                        NAMED_SHARES)

        tasks = self._get_tasks(params)

        conf_options = {MC_OPT : 'y'}
        if params['timer_merging']:
            conf_options[TM_OPT] = 'y'
        if params['redirect']:
            if not params['release_master']:
                log_once("Forcing release master option to enable redirection.")
                params['release_master'] = 'y'
            conf_options[RD_OPT] = 'y'
        if params['slack_stealing']:
            conf_options[SS_OPT] = 'y'

        scales = []
        for index, level in enumerate('abc'):
            if tasks['lvl%s'%level]:
                scales += [('scale%s' % level, self.__get_scale(params, index))]

        schedule_variables = params.items() + tasks.items() + scales
        param_variables = params.items() + [('config-options',conf_options)]

        self._customize(tasks, params)

        self._write_schedule(dict(schedule_variables))
        self._write_params(dict(param_variables))

        # Ugly
        del(self.shares)


# Types are base, locking, preemptive
# This sets up the scheduler to create each
TP_TYPE = """#if $type != 'unmanaged'
/proc/sys/litmus/color/lock_cache{1}
#else
/proc/sys/litmus/color/lock_cache{0}
#end if
#if $type == 'scheduling'
/proc/sys/litmus/color/preempt_cache{1}
#else
/proc/sys/litmus/color/preempt_cache{0}
#end if"""

# Always add some pages
TP_ADD = """/proc/sys/litmus/color/add_pages{1}"""

# Use special spin for color tasks
TP_COLOR_BASE = """colorspin -y $t.id -x $t.colorcsv -q $t.wss -l $t.loops """

TP_COLOR_B = TP_BASE.format("b", TP_COLOR_BASE + "-p $t.cpu ")
TP_COLOR_C = TP_BASE.format("c", TP_COLOR_BASE)

# Not even sure job splitting is still possible
TP_CHUNK = """#if $chunk_size > 0
/proc/sys/litmus/color/chunk_size{$chunk_size}
#end if"""

COLOR_TYPES = ['scheduling', 'locking', 'unmanaged']

class ColorMcGenerator(McGenerator):
    __SINGLE_PAGE_LOOP_MS = {'ringo': .023}

    def __init__(self, params = {}):
        super(ColorMcGenerator, self).__init__("MC",
            templates=[TP_ADD, TP_TYPE, TP_CHUNK, TP_COLOR_B, TP_COLOR_C],
            options=self.__make_options(),
            params=self.__extend_params(params))

        self.tasksets = None

    def __extend_params(self, params):
        '''Add in fixed mixed-criticality parameters.'''
        params['levels'] = 2
        params['be'] = False
        params['redirect'] = True
        params['release_master'] = True
        params['timer_merging'] = False
        params['slack_stealing'] = False

        # Set these just so they aren't displayed to the user
        params['d_nice'] = False
        params['d_fifo'] = False
        params['a_hyperperiod'] = 0
        params['a_utils'] = 'bimo-light'

        return params

    def __get_system_name(self):
        import socket
        return socket.gethostname().split(".")[0]

    def __make_system_info(self):
        info = get_cache_info()

        if not info:
            # Pick something semi-reasonable. these will work (wastefully) on
            # many machines. The plugin will pidgeon hole pages into these
            # specific areas, so even if the cache which runs this code has
            # more ways and/or colors than these, it will run as though these
            # are its parameters. This is sufficient for most testing
            ways   = 8
            colors = 8
            page   = 4096
            line   = 64

            cache = ways * colors * page
            sets  = cache / (line * ways)

            info = CacheInfo(cache, line=line, page=page,
                             ways=ways, sets=sets, colors=colors)

        self.cache = info

        hostname = self.__get_system_name()
        if hostname not in self.__SINGLE_PAGE_LOOP_MS:
            first_host = self.__SINGLE_PAGE_LOOP_MS.keys()[0]
            log_once("hostname", "No timing info for host %s" % hostname +
                     ", needed to calculate work done per task. Please get the "
                     "timing info and add to __SINGLE_PAGE_LOOP_MS in " +
                     "mc_generators.py. Assuming host %s." % first_host)
            hostname = first_host
        self.host = hostname

    def __make_options(self):
        self.__make_system_info()

        return [GenOption('type', COLOR_TYPES, COLOR_TYPES,
                          'Cache management type.'),
                GenOption('host', self.__SINGLE_PAGE_LOOP_MS.keys(), self.host,
                          'System experiment will run on (for calculating work).'),
                GenOption('chunk_size_ns', float, 0, 'Chunk size. 0 = no chunking.'),
                GenOption('ways', int, self.cache.ways, 'Ways (associativity).'),
                GenOption('colors', int, self.cache.colors,
                          'System colors (cache size / ways).'),
                GenOption('page_size', int, self.cache.page,
                          'System page size.'),
                GenOption('wss', [float, int], .5,
                          'Task working set sizes. Can be expressed as a fraction ' +
                          'of the cache.')]


    def __get_wss_pages(self, params):
        '''Return the number of pages in a single task's working set.'''
        cache_pages = params['ways'] * params['colors']

        wss = params['wss']
        if type(wss) == float and wss <= 1.0:
            # Can express wss as fraction of total cache
            pages = int(wss*cache_pages)
        else:
            if wss < params['page_size']:
                raise Exception(('Cannot have working set (%d) smaller than '
                                 'a page (%d).') % (wss, params['page_size']))

            pages = wss / params['page_size']

            if pages > cache_pages:
                raise Exception('WSS (%d) larger than the cache!' % (wss))

        return pages


    def __make_csv(self, task):
        '''Write task.colors into a csv file, stored as task.colorcsv.'''
        fname = 'colors%d.csv' % task.id
        task.colorcsv = fname

        with open(self._out_dir() + "/" + fname, 'w') as f:
            for color, replicas in task.colors.iteritems():
                f.write("%d, %d\n" % (color, replicas))

    def __get_loops(self, task, pages, system):
        all_pages_loop = self.__SINGLE_PAGE_LOOP_MS[system] * pages
        return int(task.cost / all_pages_loop) + 1

    def _get_tasks(self, params):
        # Share tasksets amongst experiments with different types but
        # identical other parameters for proper comparisons
        if self.tasksets == None:
            fields = params.keys()
            fields.remove("type")
            self.tasksets = TupleTable( ColMap(fields), lambda:None )

        if params not in self.tasksets:
            ts = super(ColorMcGenerator, self)._get_tasks(params)
            self.tasksets[params] = ts

        return self.tasksets[params]

    def _customize(self, task_system, params):
        '''Add coloring properties to the mixed-criticality task system.'''
        pages_needed = self.__get_wss_pages(params)
        real_wss = params['page_size'] * pages_needed

        # Every task needs a unique id for coloring and wss walk order
        all_tasks = []
        for level, tasks in task_system.iteritems():
            all_tasks += tasks
        for i, task in enumerate(all_tasks):
            task.id  = i
            task.wss = real_wss
            task.loops = self.__get_loops(task, pages_needed, params['host'])

        c = params['colors']
        w = params['ways']

        if params['type'] == 'unmanaged':
            hrt_colorer = EvilColorScheme(c, w)
            srt_colorer = hrt_colorer
        else:
            srt_colorer = RandomColorScheme(c, w)
            hrt_colorer = BlockColorScheme(c, w, way_first=True)

        hrt_colorer.color(task_system['lvlb'], pages_needed)
        srt_colorer.color(task_system['lvlc'], pages_needed)

        for t in all_tasks:
            self.__make_csv(t)
