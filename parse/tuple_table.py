from Cheetah.Template import Template
from collections import defaultdict,namedtuple
from point import SummaryPoint,Type
from dir_map import DirMap
from col_map import ColMap,ColMapBuilder
from pprint import pprint

class TupleTable(object):
    def __init__(self, col_map, default=lambda:[]):
        self.col_map = col_map
        self.table = defaultdict(default)

    def get_col_map(self):
        return self.col_map

    def __bool__(self):
        return bool(self.table)
    __nonzero__ = __bool__

    def __getitem__(self, kv):
        key = self.col_map.get_key(kv)
        return self.table[key]

    def __setitem__(self, kv, value):
        key = self.col_map.get_key(kv)
        self.table[key] = value

    def __contains__(self, kv):
        key = self.col_map.get_key(kv)
        return key in self.table

    def __iter__(self):
        return self.table.iteritems()

    def reduce(self):
        reduced = ReducedTupleTable(self.col_map)
        for key, value in self.table.iteritems():
            if type(value) == type([]):
                value = SummaryPoint(value[0].id, value)
            reduced.table[key] = value
        return reduced

    def __str__(self):
        s = str(Template("""ColMap: $col_map
        #for $item in $table
        $item :$table[$item]
        #end for""", searchList=vars(self)))
        return s

class ReducedTupleTable(TupleTable):
    def __init__(self, col_map):
        super(ReducedTupleTable, self).__init__(col_map, default=SummaryPoint)

    def __add_to_dirmap(self, dir_map, variable, kv, point):
        value = kv.pop(variable)

        for stat in point.get_stats():
            summary = point[stat]

            for summary_type in Type:
                measurement = summary[summary_type]

                for base_type in Type:
                    if not base_type in measurement:
                        continue
                    # Ex: release/num_tasks/measured-max/avg/x=5.csv
                    leaf = (self.col_map.encode(kv) or "line") + ".csv"
                    path = [ stat, variable, base_type, summary_type, leaf ]
                    result = measurement[base_type]

                    dir_map.add_values(path, [(value, result)])

        kv[variable] = value

    def to_dir_map(self):
        dir_map = DirMap()

        for key, point in self.table.iteritems():
            kv = self.col_map.get_kv(key)

            for col in self.col_map.columns():
                val = kv[col]

                try:
                    float(str(val))
                except:
                    # Only vary numbers. Otherwise, just have seperate files
                    continue

                self.__add_to_dirmap(dir_map, col, kv, point)

        dir_map.remove_childless()
        return dir_map

    @staticmethod
    def from_dir_map(dir_map):
        Leaf = namedtuple('Leaf', ['stat', 'variable', 'base',
                                   'summary', 'config', 'values'])

        def leafs():
            for path, node in dir_map.leafs():
                # The path will be of at least size 1: the filename
                leaf = path.pop()

                base = path.pop() if (path and path[-1] in Type) else Type.Avg
                summ = path.pop() if (path and path[-1] in Type) else Type.Avg

                path += ['?', '?'][len(path):]

                [stat, variable] = path

                config_str = leaf[:leaf.index('.csv')]
                config = ColMap.decode(config_str)

                leaf = Leaf(stat, variable, base, summ,
                            config, node.values)
                yield leaf

        builder = ColMapBuilder()

        # Gather all possible config values for ColMap
        for leaf_deets in leafs():
            for k, v in leaf_deets.config.iteritems():
                builder.try_add(k, v)

        col_map = builder.build()
        table = ReducedTupleTable(col_map)

        # Set values at each point
        for leaf in leafs():
            for (x, y) in leaf.values:
                leaf.config[leaf.variable] = str(x)
                summary = table[leaf.config][leaf.stat]
                summary[leaf.summary][leaf.base] = y

        return table

    def write_map(self, out_map):
        rows = {}

        for key, point in self.table.iteritems():
            row = {}
            for name,measurement in point:
                name = name.lower().replace('_','-')
                row[name]={}
                for base_type in Type:
                    type_key = str(base_type).lower()
                    if base_type in measurement[Type.Avg]:
                        value = measurement[Type.Avg][base_type]
                        row[name][type_key] = value
            rows[key] = row

        result = {'columns': self.col_map.columns(), 'rows':rows}

        with open(out_map, 'wc') as map_file:
            pprint(result,stream=map_file, width=20)

