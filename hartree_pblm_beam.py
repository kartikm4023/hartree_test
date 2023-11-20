import apache_beam as beam
from typing import Tuple, TypeVar
from apache_beam.options.pipeline_options import PipelineOptions
from collections import namedtuple


K = TypeVar('K')
V = TypeVar('V')

def parse_file1(line):
    """ Parse Dataset1 """
    items = line.split(',')
    return items[2], items

def parse_file2(line):
    """ Parse Dataset2 """
    items = line.split(',')
    return items[0], items

def convert_num(row):
    return [int(field) if field.isdigit() else field for field in row]

def parse_dataset(element) -> Tuple[K, V]:
    (key, data) = element
    dataset1 = data['dataset1']
    dataset2 = data['dataset2'][0]
    for record in dataset1:
        record = convert_num(record)
        yield ((record[1], record[2], dataset2[1]) , (record[3], record[4], record[5]))

def calculate_aggregates(element):
    (key, values) = element
    counterparty = key
    max_rating = max(values)
    sum_value_ARAP = sum(value for rating, status, value, tier in values if status == 'ARAP')
    sum_value_ACCR = sum(value for rating, status, value, tier in values if status == 'ACCR')
    tier = values[0][3]
    yield (counterparty, max_rating, sum_value_ARAP, sum_value_ACCR, tier)


def calculate_sums(element):
    (key, values) = element
    (legal_entity, counter_party, tier) = key 
    max_rating = max(map(lambda x: x[0], values))
    sum_value_arap = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ARAP', values)))
    sum_value_accr = sum(map(lambda x: x[2], filter(lambda x: x[1] == 'ACCR', values)))

    return (legal_entity, counter_party, tier, max_rating, sum_value_arap, sum_value_accr)
    

Result = namedtuple('Result', ['legal_entity', 'counter_party', 'tier', 'rating', 'sum_ARAP', 'sum_ACCR'])

def convert_to_namedtuple(elements):
    return Result(*elements)

with beam.Pipeline(options=PipelineOptions()) as p:
    dataset1 = (
        p
        | "Read OrderData" >> beam.io.ReadFromText('./data/dataset1.csv')
        | "OrderData To Tuple" >> beam.Map(parse_file1)
    )

    dataset2 = (
        p
        | "Read CustomerData" >> beam.io.ReadFromText('./data/dataset2.csv')
        | "CustomerData To Tuple" >> beam.Map(parse_file2)
    )

    aggregated = ({'dataset1': dataset1, 'dataset2': dataset2} 
            | 'Combine datasets' >> beam.CoGroupByKey()
            | 'Convert Values' >> beam.FlatMap(parse_dataset)
            | 'Grouping Key' >> beam.GroupByKey()
            | 'Calculate Sums' >> beam.Map(calculate_sums)
            | 'Format output' >> beam.Map(convert_to_namedtuple)
            | 'Final Output' >> beam.io.WriteToText('./data/beam_results', file_name_suffix='.csv'))
