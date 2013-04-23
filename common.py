import os
import re
import stat
import subprocess
import sys

from collections import defaultdict
from textwrap import dedent

def get_executable(prog, cwd="."):
    '''Search for @prog in system PATH and @cwd.'''

    cwd_path = "%s/%s" % (cwd, prog)
    if is_executable(cwd_path):
        return cwd_path
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, prog)
            if is_executable(exe_file):
                return exe_file

    full_cwd = os.path.abspath(cwd)
    raise IOError("Cannot find executable '%s'! (cwd='%s')" % (prog, full_cwd))

def get_executable_hint(prog, hint, optional=False):
    '''Search for @prog in system PATH. Print @hint if no binary is found.
    Die if not @optional.'''
    try:
        full_path = get_executable(prog)
    except IOError:
        if not optional:
            sys.stderr.write(("Cannot find executable '%s' in PATH. This is " +\
                              "a part of '%s' which should be added to PATH.\n")\
                             % (prog, hint))
            sys.exit(1)
        else:
            full_path = None

    return full_path

def get_config_option(option):
    '''Search for @option in installed kernel config (if present).
    Raise an IOError if the kernel config isn't found in /boot/.'''
    uname = subprocess.check_output(["uname", "-r"]).strip()
    fname = "/boot/config-%s" % uname

    if os.path.exists(fname):
        config_regex = "^CONFIG_{}=(?P<val>.*)$".format(option)
        with open(fname, 'r') as f:
            data = f.read()
        match = re.search(config_regex, data, re.M)
        if not match:
            return None
        else:
            return match.group("val")

    else:
        raise IOError("No config file exists!")

def try_get_config_option(option, default):
    try:
        get_config_option(option)
    except:
        return default

def recordtype(typename, field_names, default=0):
    ''' Mutable namedtuple. Recipe from George Sakkis of MIT.'''
    field_names = tuple(map(str, field_names))
    # Create and fill-in the class template
    numfields = len(field_names)
    argtxt = ', '.join(field_names)
    reprtxt = ', '.join('%s=%%r' % f for f in field_names)
    dicttxt = ', '.join('%r: self.%s' % (f,f) for f in field_names)
    tupletxt = repr(tuple('self.%s' % f for f in field_names)).replace("'",'')
    inittxt = '; '.join('self.%s=%s' % (f,f) for f in field_names)
    itertxt = '; '.join('yield self.%s' % f for f in field_names)
    eqtxt   = ' and '.join('self.%s==other.%s' % (f,f) for f in field_names)
    template = dedent('''
        class %(typename)s(object):
            '%(typename)s(%(argtxt)s)'

            __slots__  = %(field_names)r

            def __init__(self, %(argtxt)s):
                %(inittxt)s

            def __len__(self):
                return %(numfields)d

            def __iter__(self):
                %(itertxt)s

            def __getitem__(self, index):
                return getattr(self, self.__slots__[index])

            def __setitem__(self, index, value):
                return setattr(self, self.__slots__[index], value)

            def todict(self):
                'Return a new dict which maps field names to their values'
                return {%(dicttxt)s}

            def __repr__(self):
                return '%(typename)s(%(reprtxt)s)' %% %(tupletxt)s

            def __eq__(self, other):
                return isinstance(other, self.__class__) and %(eqtxt)s

            def __ne__(self, other):
                return not self==other

            def __getstate__(self):
                return %(tupletxt)s

            def __setstate__(self, state):
                %(tupletxt)s = state
    ''') % locals()
    # Execute the template string in a temporary namespace
    namespace = {}
    try:
        exec template in namespace
    except SyntaxError as e:
        raise SyntaxError(e.message + ':\n' + template)
    cls = namespace[typename]

    # Setup defaults
    init_defaults = tuple(default for f in field_names)
    cls.__init__.im_func.func_defaults = init_defaults

    # For pickling to work, the __module__ variable needs to be set to the frame
    # where the named tuple is created.  Bypass this step in environments where
    # sys._getframe is not defined (Jython for example).
    if hasattr(sys, '_getframe') and sys.platform != 'cli':
        cls.__module__ = sys._getframe(1).f_globals['__name__']

    return cls

def load_params(fname):
    params = defaultdict(int)
    with open(fname, 'r') as f:
        data = f.read()
    try:
        params = eval(data)
    except Exception as e:
        raise IOError("Invalid param file: %s\n%s" % (fname, e))

    return params


def num_cpus():
    '''Return the number of CPUs in the system.'''

    lnx_re = re.compile(r'^(processor|online)')
    cpus = 0

    with open('/proc/cpuinfo', 'r') as f:
        for line in f:
            if lnx_re.match(line):
                cpus += 1
    return cpus

def ft_freq():
    umachine = subprocess.check_output(["uname", "-m"])

    if re.match("armv7", umachine):
        # Arm V7s use a millisecond timer
        freq = 1000.0
    elif re.match("x86", umachine):
        # X86 timer is equal to processor clock
        reg = re.compile(r'^cpu MHz\s*:\s*(?P<FREQ>\d+)', re.M)
        with open('/proc/cpuinfo', 'r') as f:
            data = f.read()

        match = re.search(reg, data)
        if not match:
            raise Exception("Cannot parse CPU frequency from x86 CPU!")
        freq = int(match.group('FREQ'))
    else:
        # You're on your own
        freq = 0
    return freq


def kernel():
    return subprocess.check_output(["uname", "-r"])

def is_executable(fname):
    '''Return whether the file passed in is executable'''
    return os.path.isfile(fname) and os.access(fname, os.X_OK)

def is_device(dev):
    if not os.path.exists(dev):
        return False
    mode = os.stat(dev)[stat.ST_MODE]
    return not (not mode & stat.S_IFCHR)

__logged = []

def set_logged_list(logged):
    global __logged
    __logged = logged

def log_once(id, msg = None, indent = True):
    global __logged

    msg = msg if msg else id

    if id not in  __logged:
        __logged += [id]
        if indent:
            msg = '   ' + msg.strip('\t').replace('\n', '\n\t')
        sys.stderr.write('\n' + msg.strip('\n') + '\n')
