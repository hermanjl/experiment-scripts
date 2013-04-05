import gen.rv as rv
import os
import pprint
import schedcat.generator.tasks as tasks
import shutil as sh

from Cheetah.Template import Template
from common import get_config_option,num_cpus,recordtype
from config.config import DEFAULTS,PARAMS
from gen.dp import DesignPointGenerator
from parse.col_map import ColMapBuilder

NAMED_PERIODS = {
    'harmonic'            : rv.uniform_choice([25, 50, 100, 200]),
    'uni-short'           : rv.uniform_int( 3,  33),
    'uni-moderate'        : rv.uniform_int(10, 100),
    'uni-long'            : rv.uniform_int(50, 250),
}

NAMED_UTILIZATIONS = {
    'uni-very-light': rv.uniform(0.0001, 0.001),
    'uni-light'     : rv.uniform(0.001, 0.1),
    'uni-medium'    : rv.uniform(  0.1, 0.4),
    'uni-heavy'     : rv.uniform(  0.5, 0.9),
    'uni-mixed'     : rv.uniform(0.001, .4),

    'exp-light'     : rv.exponential(0, 1, 0.10),
    'exp-medium'    : rv.exponential(0, 1, 0.25),
    'exp-heavy'     : rv.exponential(0, 1, 0.50),

    'bimo-light'    : rv.multimodal([(rv.uniform(0.001, 0.5), 8),
                                     (rv.uniform(  0.5, 0.9), 1)]),
    'bimo-medium'   : rv.multimodal([(rv.uniform(0.001, 0.5), 6),
                                     (rv.uniform(  0.5, 0.9), 3)]),
    'bimo-heavy'    : rv.multimodal([(rv.uniform(0.001, 0.5), 4),
                                     (rv.uniform(  0.5, 0.9), 5)]),
}

'''Components of Cheetah template for schedule file'''
TP_RM = """#if $release_master
release_master{0}
#end if"""

GenOptionT = recordtype('GenOption', ['name', 'types', 'default', 'help', 'hidden'])
def GenOption(name, types, default, help, hidden = False):
    return GenOptionT(name, types, default, help, hidden)

class Generator(object):
    '''Creates all combinations @options specified by @params.

    This class also performs checks of parameter values and prints out help.
    All subclasses must implement _create_exp.
    '''
    def __init__(self, name, templates, options, params):
        self.__make_defaults(params)

        self.options = self.__make_options(params) + options

        self.__setup_params(params)

        self.params    = params
        self.template  = "\n".join([TP_RM] + templates)
        self.scheduler = name

    def __make_defaults(self, params):
        '''Guess defaults using the properties of this computer'''
        if 'cpus' in params:
            self.cpus = min(map(int, params['cpus']))
        else:
            self.cpus = num_cpus()
        try:
            config = get_config_option("RELEASE_MASTER") and True
        except:
            config = False
        self.release_master = list(set([False, config]))


    def __make_options(self, params):
        '''Return generic Litmus options.'''
        return [GenOption('num_tasks', int,
                          range(self.cpus, 5*self.cpus, self.cpus),
                          'Number of tasks per experiment.'),
                GenOption('cpus', int, [self.cpus],
                          'Number of processors on target system.'),
                GenOption('release_master', [True,False], self.release_master,
                          'Redirect release interrupts to a single CPU.'),
                GenOption('duration', float, [30], 'Experiment duration.')]

    def _num_cpus(self):
        return self.cpus

    @staticmethod
    def _dist_option(name, default, distribution, help):
        return GenOption(name, [str, float, type([])] + distribution.keys(),
                         default, help)

    def _create_dist(self, name, value, named_dists):
        '''Attempt to create a distribution representing the data in @value.
        If @value is a string, use it as a key for @named_dists.'''
        # A list of values
        if type(value) == type([]):
            map(lambda x : self.__check_value(name, x, [float, int]), value)
            return rv.uniform_choice(value)
        elif type(value) in [float, int]:
            return lambda : value
        elif named_dists and value in named_dists:
            return named_dists[value]
        else:
            raise ValueError("Invalid %s value: %s" % (name, value))

    def _create_taskset(self, params, periods, utils, max_util = None):
        tg = tasks.TaskGenerator(period=periods, util=utils)
        ts = []
        tries = 0
        while len(ts) != params['tasks'] and tries < 100:
            ts = tg.make_task_set(max_tasks = params['tasks'], max_util=max_util)
            tries += 1
        if len(ts) != params['tasks']:
            print(("Only created task set of size %d < %d for params %s. " +
                   "Switching to light utilization.") %
                  (len(ts), params['tasks'], params))
            print("Switching to light util. This usually means the " +
                  "utilization distribution is too agressive.")
            return self._create_taskset(params, periods, NAMED_UTILIZATIONS['uni-light'],
                                        max_util)
        return ts

    def _out_dir(self):
        return self.out_dir

    def _write_schedule(self, params):
        '''Write schedule file using current template for @params.'''
        sched_file = self.out_dir + "/" + DEFAULTS['sched_file']
        with open(sched_file, 'wa') as f:
            f.write(str(Template(self.template, searchList=[params])))


    def _write_params(self, params):
        '''Write out file with relevant parameters.'''
        # Don't include this in the parameters. It will be automatically added
        # in run_exps.py
        if 'tasks' in params:
            tasks = params.pop('tasks')
        else:
            tasks = 0

        exp_params_file = self.out_dir + "/" + DEFAULTS['params_file']
        with open(exp_params_file, 'wa') as f:
            params['scheduler'] = self.scheduler
            pprint.pprint(params, f)

        if tasks:
            params['tasks'] = tasks

    def __setup_params(self, params):
        '''Set default parameter values and check that values are valid.'''
        for option in self.options:
            if option.name not in params:
                params[option.name] = option.default
            else:
                option.hidden = True
            params[option.name] = self._check_value(option.name,
                                                    option.types,
                                                    params[option.name])
        return params


    def _check_value(self, name, types, val):
        '''Raise an exception if the value of type of @val is not specified
        in @types. Returns a copy of @val with strings converted to raw
        Python types, if possible.'''
        if types == float:
            types = [float, int]
        if type(types) != type([]):
            types = [types]
        if type(val) != type([]):
            val = [val]

        retval = []
        for v in val:
            # Has to be a better way to find this
            v = False if v in ['f', 'False', 'false', 'n', 'no']  else v
            v = True  if v in ['t', 'True',  'true',  'y', 'yes'] else v

            if type(v) not in types and v not in types:
                # Try and convert v to one of the specified types
                parsed = None
                for t in types:
                    try:
                        parsed = t(v)
                        break
                    except:
                        pass

                if parsed:
                    retval += [parsed]
                else:
                    raise TypeError("Invalid %s value: '%s'" % (name, v))
            else:
                retval += [v]
        return retval

    def _create_exp(self, exp_params, out_dir):
        '''Overridden by subclasses.'''
        raise NotImplementedError

    def create_exps(self, out_dir, force, trials):
        '''Create experiments for all possible combinations of params in
        @out_dir. Overwrite existing files if @force is True.'''
        builder = ColMapBuilder()

        # Track changing values so only relevant parameters are included
        # in directory names
        for dp in DesignPointGenerator(self.params):
            for k, v in dp.iteritems():
                builder.try_add(k, v)
        col_map = builder.build()

        for dp in DesignPointGenerator(self.params):
            for trial in xrange(trials):
                # Create directory name from relevant parameters
                dir_leaf  = "sched=%s_%s" % (self.scheduler, col_map.encode(dp))
                dir_leaf  = dir_leaf.strip('_') # If there are none
                dir_leaf += ("_trial=%s" % trial) if trials > 1 else ""

                dir_path  = "%s/%s" % (out_dir, dir_leaf.strip('_'))

                if os.path.exists(dir_path):
                    if force:
                        sh.rmtree(dir_path)
                    else:
                        print("Skipping existing experiment: '%s'" % dir_path)
                        continue

                os.mkdir(dir_path)

                if trials > 1:
                    dp[PARAMS['trial']] = trial
                self.out_dir = dir_path

                self._create_exp(dict(dp))

                del(self.out_dir)
                if PARAMS['trial'] in dp:
                    del dp[PARAMS['trial']]

    HELP_INDENT = 17

    def print_help(self):
        display_options = [o for o in self.options if not o.hidden]
        s = str(Template("""scheduler $scheduler:
        #for $o in $options
        $o.name -- $o.help
        \tDefault: $o.default
        \tAllowed: $o.types
        #end for""", searchList={'scheduler':self.scheduler, 'options':display_options}))

        # Has to be an easier way to print this out...
        for line in s.split("\n"):
            res = []
            i = 0
            for word in line.split(", "):
                i += len(word)
                res += [word]
                if i > 80 and len(word) < 80:
                    print(", ".join(res[:-1]))
                    res = [" "*Generator.HELP_INDENT +res[-1]]
                    i = Generator.HELP_INDENT + len(word)
            print(", ".join(res))


generators = {}

def register_generator(name, clazz):
    generators[name] = clazz

def get_generators():
    return generators
