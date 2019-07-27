import apache_beam as beam


class Join(beam.PTransform):
    """Composite Transform to implement left/right/inner/outer sql-like joins on
    two PCollections. Columns to join can be a single column or multiple columns"""

    def __init__(self, left_pcol_name, left_pcol, right_pcol_name, right_pcol, join_type, join_keys):
        """

        :param left_pcol_name (str): Name of the first PCollection (left side table in a sql join)
        :param left_pcol (Pcollection): first PCollection
        :param right_pcol_name: Name of the second PCollection (right side table in a sql join)
        :param right_pcol (Pcollection): second PCollection
        :param join_type (str): how to join the two PCollections, must be one of ['left','right','inner','outer']
        :param join_keys (dict): dictionary of two (k,v) pairs, where k is pcol name and
            value is list of column(s) you want to perform join
        """

        self.right_pcol_name = right_pcol_name
        self.left_pcol = left_pcol
        self.left_pcol_name = left_pcol_name
        self.right_pcol = right_pcol
        if not isinstance(join_keys, dict):
            raise TypeError("Column names to join on should be of type dict. Provided one is {}".format(type(join_keys)))
        if not join_keys:
            raise ValueError("Column names to join on is empty. Provide atleast one value")
        elif len(join_keys.keys()) != 2 or set([left_pcol_name, right_pcol_name]) - set(join_keys.keys()):
            raise ValueError("Column names to join should be a dictionary of two (k,v) pairs, where k is pcol name and "
                             "value is list of column(s) you want to perform join")
        else:
            self.join_keys = join_keys
        join_methods = {
            "left": UnnestLeftJoin,
            "right": UnnestRightJoin,
            "inner": UnnestInnerJoin,
            "outer": UnnestOuterJoin
        }
        try:
            self.join_method = join_methods[join_type]
        except KeyError:
            raise Exception("Provided join_type is '{}'. It should be one of {}".format(join_type, join_methods.keys()))

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, join_keys):
            return [data_dict[key] for key in join_keys], data_dict

        return ({pipeline_name: pcoll
                | 'Convert to ([join_keys], elem) for {}'.format(pipeline_name)
                    >> beam.Map(_format_as_common_key_tuple, self.join_keys[pipeline_name]) for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest CoGrouped' >> beam.ParDo(self.join_method(), self.left_pcol_name, self.right_pcol_name)
                )


class UnnestLeftJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records from the left pcol, and the matched records from the right pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how a left join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        for source_dictionary in source_dictionaries:
            if join_dictionaries:
                # For each matching row from the join table, update the source row
                for join_dictionary in join_dictionaries:
                    source_dictionary.update(join_dictionary)
                    yield source_dictionary
            else:
                # if there are no rows matching from the join table, yield the source row as it is
                yield source_dictionary


class UnnestRightJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records from the right pcol, and the matched records from the left pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how a right join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        for join_dictionary in join_dictionaries:
            if source_dictionaries:
                for source_dictionary in source_dictionaries:
                    join_dictionary.update(source_dictionary)
                    yield join_dictionary
            else:
                yield join_dictionary


class UnnestInnerJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records that have matching values in both the pcols"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how an inner join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        if not source_dictionaries or not join_dictionaries:
            pass
        else:
            for source_dictionary in source_dictionaries:
                for join_dictionary in join_dictionaries:
                    source_dictionary.update(join_dictionary)
                    yield source_dictionary


class UnnestOuterJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records when there is a macth in either left pcol or right pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how an outer join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        if source_dictionaries:
            if join_dictionaries:
                for source_dictionary in source_dictionaries:
                    for join_dictionary in join_dictionaries:
                        source_dictionary.update(join_dictionary)
                        yield source_dictionary
            else:
                for source_dictionary in source_dictionaries:
                    yield source_dictionary
        else:
            for join_dictionary in join_dictionaries:
                yield join_dictionary
