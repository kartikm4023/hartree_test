import apache_beam as beam
from typing import Tuple, TypeVar
from apache_beam.options.pipeline_options import PipelineOptions
from collections import namedtuple


K = TypeVar('K')
V = TypeVar('V')


def parse_dataset(line, dataset_name):
    """ Parse a dataset """
    items = line.split(',')
    if dataset_name == 'dataset1':
        return items[2], items
    elif dataset_name == 'dataset2':
        return items[0], items
    else:
        raise ValueError("Invalid dataset name")


def convert_num(row):
    return [int(field) if field.isdigit() else field for field in row]


def preprocess_dataset(element, arg_condition) -> Tuple[K, V]:
    (key, data) = element
    dataset1 = data['dataset1']
    dataset2 = data['dataset2'][0]
    if arg_condition == 'ALL':
        dataset2 = data['dataset2'][0]
        for record in dataset1:
            record = convert_num(record)
            yield ((record[1], record[2], dataset2[1]), (record[3], record[4], record[5]))
    elif arg_condition == 'LE':
        for record in dataset1:
            record = convert_num(record)
            yield ((record[1]), (record[3], record[4], record[5]))
    elif arg_condition == 'CP':
        for record in dataset1:
            record = convert_num(record)
            yield ((record[2]), (record[3], record[4], record[5]))
    elif arg_condition == 'TIER':
        dataset2 = data['dataset2'][0]
        for record in dataset1:
            record = convert_num(record)
            yield ((dataset2[1]), (record[3], record[4], record[5]))


def calculate_aggs(element, arg_condition):
    (key, values) = element
    if arg_condition == 'ALL':
        (legal_entity, counter_party, tier) = key
        max_rating = max(map(lambda x: x[0], values))
        sum_value_arap = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ARAP', values)))
        sum_value_accr = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ACCR', values)))
        return (legal_entity, counter_party, tier, max_rating, sum_value_arap, sum_value_accr)
    elif arg_condition == 'LE':
        legal_entity = key
        max_rating = max(map(lambda x: x[0], values))
        sum_value_arap = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ARAP', values)))
        sum_value_accr = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ACCR', values)))
        return (legal_entity, 'Total', 'Total', max_rating, sum_value_arap, sum_value_accr)
    elif arg_condition == 'CP':
        counter_party = key 
        max_rating = max(map(lambda x: x[0], values))
        sum_value_arap = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ARAP', values)))
        sum_value_accr = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ACCR', values)))
        return ('Total', counter_party, 'Total', max_rating, sum_value_arap, sum_value_accr)
    elif arg_condition == 'TIER':
        tier = key 
        max_rating = max(map(lambda x: x[0], values))
        sum_value_arap = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ARAP', values)))
        sum_value_accr = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ACCR', values)))
        return ('Total', 'Total', tier, max_rating, sum_value_arap, sum_value_accr)


Result = namedtuple('Result', ['legal_entity', 'counter_party', 'tier', 'rating', 'sum_ARAP', 'sum_ACCR'])


def convert_to_namedtuple(elements):
    return Result(*elements)


with beam.Pipeline(options=PipelineOptions()) as p:

    dataset1 = (
        p
        | "Read Dataset1" >> beam.io.ReadFromText('./data/dataset1.csv')
        | "OrderData To Tuple" >> beam.Map(lambda x: parse_dataset(x, 'dataset1'))
    )

    dataset2 = (
        p
        | "Read Dataset2" >> beam.io.ReadFromText('./data/dataset2.csv')
        | "CustomerData To Tuple" >> beam.Map(lambda x: parse_dataset(x, 'dataset2'))
    )

    combined_dataset = ({'dataset1': dataset1, 'dataset2': dataset2}
        | 'Combine datasets' >> beam.CoGroupByKey())
    
    all_group = (combined_dataset
        | 'Parse all dataset' >> beam.FlatMap(lambda x: preprocess_dataset(x, 'ALL'))
        | 'Grouping Key' >> beam.GroupByKey()
        | 'Calculate aggregates' >> beam.Map(lambda x: calculate_aggs(x, 'ALL'))
        | 'Format output' >> beam.Map(convert_to_namedtuple))
    
    total_legal_entity = (combined_dataset
        | 'Parse LE dataset' >> beam.FlatMap(lambda x: preprocess_dataset(x, 'LE'))
        | 'Grouping LE Key' >> beam.GroupByKey()
        | 'Calculate LE aggregates' >> beam.Map(lambda x: calculate_aggs(x, 'LE'))
        | 'Format LE output' >> beam.Map(convert_to_namedtuple))
    
    total_counter_party = (combined_dataset
        | 'Parse CP dataset' >> beam.FlatMap(lambda x: preprocess_dataset(x, 'CP'))
        | 'Grouping CP Key' >> beam.GroupByKey()
        | 'Calculate CP aggregates' >> beam.Map(lambda x: calculate_aggs(x, 'CP'))
        | 'Format CP output' >> beam.Map(convert_to_namedtuple))
    
    total_tier = (combined_dataset
        | 'Parse Tier dataset' >> beam.FlatMap(lambda x: preprocess_dataset(x, 'TIER'))
        | 'Grouping Tier Key' >> beam.GroupByKey()
        | 'Calculate Tier aggregates' >> beam.Map(lambda x: calculate_aggs(x, 'TIER'))
        | 'Format Tier output' >> beam.Map(convert_to_namedtuple))
    
    output = ((all_group, total_legal_entity, total_counter_party, total_tier)
        | 'Flatten all outputs' >> beam.Flatten()
        | 'Print all output' >> beam.Map(print)
        | 'Final all Output' >> beam.io.WriteToText('./data/beam_results', file_name_suffix='.csv'))
