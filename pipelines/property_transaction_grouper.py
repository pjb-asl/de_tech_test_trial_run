import logging
import argparse
import csv
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class GeneratePropertyKeyDoFn(beam.DoFn):
    """
    This class transforms the json object by adding in the unique property key
    The key is a string from the combination of: postcode|paon|saon
    """
    @staticmethod
    def validate_record_fields(record):
        # ToDo: Make this more pythonic...raise exceptions
        # Need to understand exceptions and pipelines.
        if 'postcode' not in record:
            return False
        if 'paon' not in record:
            return False
        if 'saon' not in record:
            return False
        return True

    def process(self, element):
        record = json.loads(element)
        if self.validate_record_fields(record):
            key = '{postcode}|{paon}|{saon}'.format(
                postcode=record['postcode'],
                paon=record['paon'],
                saon=record['saon']
            )
            data = (key, record)
            yield beam.pvalue.TaggedOutput('success', data)
        else:
            yield beam.pvalue.TaggedOutput('error', json.dumps(record))


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    parser.add_argument(
        '--errors',
        dest='errors',
        required=True,
        help='Output file to write error rows to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        json_records = p | "Read source file" >> ReadFromText(known_args.input)
        keyed_json_records = json_records | "Add property keys" >> (beam.ParDo(GeneratePropertyKeyDoFn()).with_outputs('errors', 'success'))
        grouped_json_records = keyed_json_records.success | "Group by property key" >> beam.GroupByKey()
        grouped_json_records | "Save output to file" >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
