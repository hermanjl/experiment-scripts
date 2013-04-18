import generator as gen
import random

TP_TBASE = """#for $t in $task_set
{} $t.cost $t.period
#end for"""
TP_GLOB_TASK = TP_TBASE.format("")
TP_PART_TASK = TP_TBASE.format("-p $t.cpu")

TP_DGL_TASK = TP_TBASE.format("dglspin -r $t.mask -p $t.cpu")

class EdfGenerator(gen.Generator):
    '''Creates sporadic task sets with the most common Litmus options.'''
    def __init__(self, scheduler, templates, options, params):
        super(EdfGenerator, self).__init__(scheduler, templates,
                                           self.__make_options() + options,
                                           params)

    def __make_options(self):
        '''Return generic EDF options.'''
        return [gen.Generator._dist_option('utils', ['uni-medium'],
                                           gen.NAMED_UTILIZATIONS,
                                           'Task utilization distributions.'),
                gen.Generator._dist_option('periods', ['harmonic'],
                                           gen.NAMED_PERIODS,
                                           'Task period distributions.')]

    def _create_exp(self, exp_params):
        '''Create a single experiment with @exp_params in @out_dir.'''
        pdist = self._create_dist('period',
                                  exp_params['periods'],
                                  gen.NAMED_PERIODS)
        udist = self._create_dist('utilization',
                                  exp_params['utils'],
                                  gen.NAMED_UTILIZATIONS)

        ts = self._create_taskset(exp_params, pdist, udist)

        self._customize(ts, exp_params)

        self._write_schedule(dict(exp_params.items() + [('task_set', ts)]))
        self._write_params(exp_params)

    def _customize(self, taskset, exp_params):
        '''Configure a generated taskset with extra parameters.'''
        pass


class PartitionedGenerator(EdfGenerator):
    def __init__(self, scheduler, templates, options, params):
        super(PartitionedGenerator, self).__init__(scheduler,
            templates + [TP_PART_TASK], options, params)

    def _customize(self, taskset, exp_params):
        start = 1 if exp_params['release_master'] else 0
        # Random partition for now: could do a smart partitioning
        for t in taskset:
            t.cpu = random.randint(start, exp_params['cpus'] - 1)

class PedfGenerator(PartitionedGenerator):
    def __init__(self, params={}):
        super(PedfGenerator, self).__init__("PSN-EDF", [], [], params)

class CedfGenerator(PartitionedGenerator):
    TP_CLUSTER = "plugins/C-EDF/cluster{$level}"
    CLUSTER_OPTION = gen.GenOption('level', ['L2', 'L3', 'All'], ['L2'],
                                   'Cache clustering level.',)

    def __init__(self, params={}):
        super(CedfGenerator, self).__init__("C-EDF",
                                            [CedfGenerator.TP_CLUSTER],
                                            [CedfGenerator.CLUSTER_OPTION],
                                            params)

class GedfGenerator(EdfGenerator):
    def __init__(self, params={}):
        super(GedfGenerator, self).__init__("GSN-EDF", [TP_GLOB_TASK],
                                            [], params)

class PedfDglGenerator(EdfGenerator):
    NR_RESOURCES_OPTION = gen.GenOption('nr_resources', int, [8],
                                   'Number of resources.',)
    NR_REQUESTED_OPTION = gen.GenOption('nr_requested', int, [2],
                                   'Number of resources requested.',)

    def __init__(self, params={}):
        super(PedfDglGenerator, self).__init__("PSN-EDF",
            [TP_DGL_TASK], [], params)


    def _customize(self, taskset, exp_params):
        start = 1 if exp_params['release_master'] else 0
        # Random partition for now: could do a smart partitioning
        for t in taskset:
            t.cpu = random.randint(start, exp_params['cpus'] - 1)
            resources = random.sample(xrange(exp_params['nr_resources']), exp_params['nr_requested'])
            t.mask = list('0'*10)
            for r in resources:
                t.mask[r] = '1'
            t.mask = ''.join(t.mask)
