from common import log_once
from collections import namedtuple
from parse.tuple_table import TupleTable

import itertools
import matplotlib.pyplot as plot

class ExcessVarietyException(Exception):
    '''Too many fields or field values to use field style'''
    pass

class Style(namedtuple('SS', ['marker', 'color', 'line'])):
    def fmt(self):
        return self.marker + self.line + self.color


def make_styler(col_map):
    try:
        return FieldStyle(col_map.get_values())
    except ExcessVarietyException:
        # Fallback, don't style by field values, instead create
        # a unique style for every combination of field values possible
        # This is significantly harder to visually parse
        log_once("Too many columns and/or column values to create pretty "
                 "and simple graphs!\nGiving each combination of properties "
                 "its own line.")
        return CombinationStyle(col_map)

class StyleMap(object):
    # The base style, a solid black line
    # The values of columns are used to change this line
    DEFAULT = Style(marker='', line= '-', color='k')

    def __init__(self, col_values):
        raise NotImplementedError()

    def _all_styles(self):
        '''A dict holding all possible values for style each property.'''
        return Style(marker=list('.,ov^<>1234sp*hH+xDd|_'),
                     line=['-', ':', '--'],
                     color=list('kbgrcmy'))._asdict()

    def get_style(self, kv):
        '''Translate column values to unique line style.'''
        raise NotImplementedError()

    def get_key(self):
        '''A visual description of this StyleMap.'''
        raise NotImplementedError()


class FieldStyle(StyleMap):
    '''Changes properties of a line style by the values of each field.'''

    ORDER = [ str, bool, float, int ]

    def __init__(self, col_values):
        '''Assign (some) columns in @col_list to fields in @Style to vary, and
        assign values for these columns to specific field values.'''
        # column->map(column_value->field_value)
        self.value_map = {}
        # column->style_field
        self.field_map = {}

        if len(col_values.keys()) > len(FieldStyle.DEFAULT):
            raise ExcessVarietyException("Too many columns to style!")

        col_list   = self.__get_sorted_columns(col_values)
        field_list = self.__get_sorted_fields()
        field_dict = self._all_styles()

        while len(col_list) < len(field_list):
            curr_col = col_list[-1]
            check_field = field_list[-2]
            if len(col_values[curr_col]) <= len(field_dict[check_field]):
                field_list.pop()
            elif len(col_values[curr_col]) > len(field_dict[field_list[-1]]):
                raise ExcessVarietyException("Too many values to style!")
            else:
                field_list.pop(0)

        # Pair each column with a style field
        for i in xrange(len(col_list)):
            column = col_list[i]
            field  = field_list[i]
            field_values = field_dict[field]

            # Give each unique value of column a matching unique value of field
            value_dict  = {}
            for value in sorted(col_values[column]):
                value_dict[value] = field_values.pop(0)

            self.value_map[column] = value_dict
            self.field_map[column] = field

    def __get_sorted_columns(self, col_values):
        # Break ties using the type of the column
        def type_priority(column):
            value = col_values[column].pop()
            col_values[column].add(value)
            try:
                t = float if float(value) % 1.0 else int
            except:
                t = bool if value in ['True','False'] else str
            return StyleMap.ORDER.index(t)

        def column_compare(cola, colb):
            lena = len(col_values[cola])
            lenb = len(col_values[colb])
            if lena == lenb:
                return type_priority(cola) - type_priority(colb)
            else:
                return lena - lenb

        return sorted(col_values.keys(), cmp=column_compare)

    def __get_sorted_fields(self):
        fields = self._all_styles()
        return sorted(fields.keys(), key=lambda x: len(fields[x]))

    def get_style(self, kv):
        style_fields = {}

        for column, values in self.value_map.iteritems():
            if column not in kv:
                continue
            field = self.field_map[column]
            style_fields[field] = values[kv[column]]

        return StyleMap.DEFAULT._replace(**style_fields)

    def get_key(self):
        key = []

        for column, values in self.value_map.iteritems():
            for v in values.keys():
                sdict = dict([(column, v)])
                style = self.get_style(sdict)

                styled_line = plot.plot([], [], style.fmt())[0]
                description = "%s:%s" % (column, v)

                key += [(styled_line, description)]

        return sorted(key, key=lambda x:x[1])

class CombinationStyle(StyleMap):
    def __init__(self, col_map):
        self.col_map   = col_map
        self.kv_styles = TupleTable(col_map)
        self.kv_seen   = TupleTable(col_map, lambda:False)

        all_styles = self._all_styles()
        styles_order = sorted(all_styles.keys(),
                              key=lambda x: len(all_styles[x]),
                              reverse = True)

        # Add a 'None' option in case some lines are plotted without
        # any value specified for this kv
        column_values = col_map.get_values()
        for key in column_values.keys():
            column_values[key].add(None)

        styles_iter = self.__dict_combinations(all_styles, styles_order)
        kv_iter     = self.__dict_combinations(column_values)

        # Cycle in case there are more kv combinations than styles
        # This will be really, really ugly..
        styles_iter = itertools.cycle(styles_iter)

        for kv, style in zip(kv_iter, styles_iter):
            self.kv_styles[kv] = Style(**style)

        for kv_tup, style in self.kv_styles:
            kv = self.col_map.get_kv(kv_tup)
            if not self.kv_styles[kv]:
                raise Exception("Didn't initialize %s" % kv)

    def __dict_combinations(self, list_dict, column_order = None):
        def helper(set_columns, remaining_columns):
            if not remaining_columns:
                yield set_columns
                return

            next_column = remaining_columns.pop(0)

            for v in list_dict[next_column]:
                set_columns[next_column] = v
                for vals in helper(dict(set_columns), list(remaining_columns)):
                    yield vals

        if not column_order:
            # Just use the random order returned by the dict
            column_order = list_dict.keys()

        return helper({}, column_order)

    def get_style(self, kv):
        self.kv_seen[kv] = True
        return self.kv_styles[kv]

    def get_key(self):
        key = []

        for kv_tup, style in self.kv_styles:
            kv = self.col_map.get_kv(kv_tup)
            if not self.kv_seen[kv]:
                continue

            styled_line = plot.plot([], [], style.fmt())[0]
            description = self.col_map.encode(kv, minimum=True)

            key += [(styled_line, description)]

        return sorted(key, key=lambda x:x[1])
