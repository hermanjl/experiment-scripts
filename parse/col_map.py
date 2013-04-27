from collections import defaultdict

class ColMapBuilder(object):
    def __init__(self):
        self.value_map = defaultdict(set)

    def build(self):
        columns = sorted(self.value_map.keys(),
                         key=lambda c: (-len(self.value_map[c]), c))
        col_list = filter(lambda c : len(self.value_map[c]) > 1, columns)
        return ColMap(col_list, self.value_map)

    def try_add(self, column, value):
        self.value_map[column].add( str(value) )

    def try_remove(self, column):
        if column in self.value_map:
            del(self.value_map[column])

    def __contains__(self, col):
        return col in self.value_map

class ColMap(object):
    def __init__(self, col_list, values = None):
        self.col_list = col_list
        self.rev_map = {}
        self.values = values

        self.minimums = []
        for c in col_list:
            end = 1
            while c[:end] in self.minimums:
                end += 1
            self.minimums += [c[:end]]

        for i, col in enumerate(col_list):
            self.rev_map[col] = i

    def columns(self):
        return self.col_list

    def get_values(self):
        return self.values

    def get_key(self, kv):
        '''Convert a key-value dict into an ordered tuple of values.'''
        key = ()

        for col in self.col_list:
            if col not in kv:
                key += (None,)
            else:
                key += (kv[col],)

        return key

    def get_kv(self, key):
        '''Convert an ordered tuple of values into a key-value dict.'''
        kv = {}
        for i in range(0, len(key)):
            kv[self.col_list[i]] = key[i]
        return kv

    def encode(self, kv, minimum=False):
        '''Converted a dict into a string with items sorted according to
        the ColMap key order.'''
        def escape(val):
            return str(val).replace("_", "-").replace("=", "-")

        vals = []

        if minimum:
            format = "%s:%s"
            join = ", "
        else:
            format = "%s=%s"
            join = "_"

        reverse = list(self.col_list)
        reverse.reverse()
        for key in reverse:
            if key not in kv:
                continue
            display = key if not minimum else self.minimums[self.rev_map[key]]
            k, v = escape(display), escape(kv[key])
            vals += [format % (k, v)]

        return join.join(vals)

    @staticmethod
    def decode(string):
        '''Convert a string into a key-value dict.'''
        vals = {}
        for assignment in string.split("_"):
            k, v = assignment.split("=")
            vals[k] = v
        return vals

    def __contains__(self, col):
        return col in self.rev_map

    def __str__(self):
        return "<ColMap>%s" % (self.rev_map)
