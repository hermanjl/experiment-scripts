import gen.rv as rv

from common import try_get_config_option
from gen.generator import GenOption,Generator,NAMED_UTILIZATIONS,NAMED_PERIODS


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
$t.cpu, $t.id, $t.budget
#end for
}
#end if
""" + TP_BASE.format("a", "-i $t.id -p $t.cpu ")
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
    def __init__(self, params = {}):
        super(McGenerator, self).__init__("MC",
                                          [TP_LVLA, TP_LVLB, TP_LVLC, TP_LVLD],
                                          self.__make_options(),
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
                                       'Distribution of actual utilizations.')]

    def __partition_worst_fit(self, params, ts):
        # Partition using worst-fit for most even distribution
        utils = [0]*int(params['cpus'])
        tasks = [0]*int(params['cpus'])
        for t in ts:
            t.cpu = utils.index(min(utils))
            t.id = tasks[t.cpu]

            utils[t.cpu] += t.utilization()
            tasks[t.cpu] += 1

    def __adjust(self, params, level):
        # Adjust for levels which aren't used
        ldiff  = LEVELS - params['levels']
        shares = self.shares[ldiff:]
        level -= ldiff

        return shares, level

    def __get_max_util(self, params, level):
        shares, level = self.__adjust(params, level)
        return float(shares[level]) / sum(shares[:level]) * params['cpus']

    def __get_scale(self, params, level):
        shares, level = self.__adjust(params, level)
        return float(shares[level]) / sum(shares)

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

        # Level-A is present, b must be harmonic with lvla hyperperiod
        if params['levels'] > 2:
            plist = [params['a_hyperperiod']*2**x for x in xrange(0, 4)]
            periods = rv.uniform_choice(plist)
        else:
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

    def _create_exp(self, params):
        # Ugly way of doing it
        self.shares = self._create_dist('shares', params['shares'],
                                        NAMED_SHARES)

        tasks = {'lvla': self.__create_lvla_sched(params),
                 'lvlb': self.__create_lvlb_sched(params),
                 'lvlc': self.__create_lvlc_sched(params)}

        conf_options = {MC_OPT : 'y'}
        if params['timer_merging']:
            conf_options[TM_OPT] = 'y'
        if params['redirect']:
            if not params['release_master']:
                print("Forcing release master option to enable redirection.")
                params['release_master'] = 'y'
            conf_options[RD_OPT] = 'y'
        if params['slack_stealing']:
            conf_options[SS_OPT] = 'y'

        scales = []
        for index, level in enumerate('abc'):
            scales += [('scale%s' % level, self.__get_scale(params, index))]

        schedule_variables = params.items() + tasks.items() + scales
        param_variables = params.items() + [('config-options',conf_options)]

        self._write_schedule(dict(schedule_variables))
        self._write_params(dict(param_variables))

        # Ugly
        del(self.shares)
