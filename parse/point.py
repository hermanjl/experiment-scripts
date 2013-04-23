'''
Too much duplicate code in this file
'''

import copy
import numpy as np
from enum import Enum
from collections import defaultdict

Type = Enum(['Min','Max','Avg','Var'])
default_typemap = {Type.Max : {Type.Max : 1, Type.Min : 0, Type.Avg : 0, Type.Var : 0},
                   Type.Min : {Type.Max : 0, Type.Min : 1, Type.Avg : 0, Type.Var : 0},
                   Type.Avg : {Type.Max : 1, Type.Min : 1, Type.Avg : 1, Type.Var : 1}}

def make_typemap():
    return copy.deepcopy(default_typemap)

def dict_str(adict, sep = "\n"):
    def num_str(v):
        try:
            float(v)
            return "%6.3f" % v
        except:
            return v
    size = 20 if sep == "\n" else 4
    return sep.join([("%" + str(size) + "s: %9s") % (k, num_str(v)) for (k,v) in sorted(adict.iteritems())])

class Measurement(object):
    def __init__(self, id = None, kv = {}, default=list):
        self.id = id
        self.stats = defaultdict(default)
        for k, v in kv.iteritems():
            self[k] = v

    def from_array(self,array):
        array = np.array(array)
        self[Type.Max] = array.max()
        self[Type.Avg] = array.mean()
        self[Type.Var] = array.var()
        self[Type.Min] = array.min()
        return self

    def __check_type(self, type):
        if not type in Type:
            raise AttributeError("Not a valid type '%s'" % type)

    def __getitem__(self, type):
        self.__check_type(type)
        return self.stats[type]

    def __iter__(self):
        return self.stats.iteritems()

    def __contains__(self, type):
        self.__check_type(type)
        return type in self.stats

    def __setitem__(self, t, value):
        self.__check_type(t)
        # Numpy returns single memmapped values which can't be pickled
        # Convert them to floats which can be
        if type(value) is np.memmap:
            value = float(value)
        self.stats[t] = value

    def __str__(self):
        return "%s" % dict_str(self.stats, " ")

class Summary(Measurement):
    def __init__(self, id="", measures=[], typemap = default_typemap):
        super(Summary, self).__init__(id, default=Measurement)

        if measures:
            self.__check_types(measures, typemap)
            self.__summarize(measures, typemap)

    def __check_types(self, measures, typemap):
        required_types = self.__get_required(typemap)
        for m in measures:
            for type in required_types:
                if type not in m:
                    raise ValueError("measurement '%s' missing type '%s'" %
                                     (self.id, type))

    def __summarize(self, measures, typemap):
        for sum_type in Type:
            self[sum_type] = Measurement(self.id)

        def avg(vals):
            return sum(vals) / len(vals)

        for base_type in Type:
            for sum_type, func in (Type.Min,min),(Type.Max,max),(Type.Avg, avg):
                if typemap[sum_type][base_type]:
                    val = func([m[base_type] for m in measures])
                    self[sum_type][base_type] = val

    def __get_required(self, typemap):
        required = []
        for base_type in Type:
            matches = [t[base_type] for t in typemap.itervalues()]
            if bool(sum(matches)):
                required += [base_type]
        return required

class ExpPoint(object):
    def __init__(self, id = "", init = {}, default=Measurement):
        self.stats = defaultdict(default)
        for type, value in init.iteritems():
            self[type] = value
        self.id = id

    def __check_val(self, obj):
        if not isinstance(obj, Measurement):
            raise AttributeError("Not a valid measurement '%s'" % obj)

    def __getitem__(self, type):
        return self.stats[type]

    def __iter__(self):
        return self.stats.iteritems()

    def __contains__(self, type):
        return type in self.stats

    def __setitem__(self, type, value):
        self.__check_val(value)
        self.stats[type] = value

    def __str__(self):
        return "<ExpPoint-%s>\n%s" % (self.id, dict_str(self.stats))

    def get_stats(self):
        return self.stats.keys()

    def __bool__(self):
        return bool(self.stats)
    __nonzero__ = __bool__


class SummaryPoint(ExpPoint):
    def __init__(self, id="", points=[], typemap = default_typemap):
        super(SummaryPoint,self).__init__("Summary-%s" % id,
                                          default=Summary)

        grouped = defaultdict(lambda : [])

        for exp in points:
            for name,measure in exp.stats.iteritems():
                grouped[name] += [measure]

        for key in grouped.iterkeys():
            self[key] = Summary(key, grouped[key], typemap)
