import logging
import argparse
import csv
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class CsvToJsonDoFn(beam.DoFn):
    """
    This class transforms the csv source file into a json file
    marked up with the relevant field names
    """
    fields = ['tx_uid', 'price', 'tx_date', 'postcode', 'property_type',
              'age_classification', 'tenure_duration', 'paon', 'saon', 'street',
              'locality', 'town_city', 'district', 'county', 'ppd_category_type',
              'change_type']

    def process(self, element):
        reader = csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # ToDo: This needs a try catch so the pipeline never stops!
            if len(self.fields) == len(row):
                yield beam.pvalue.TaggedOutput('success', json.dumps(dict(zip(self.fields, row))))
            else:
                # ToDo: Turn into something that triggers the failure to be output somewhere so it can be investigated
                yield beam.pvalue.TaggedOutput('errors', json.dumps(row))


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

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        csv_records = p | "Read source file" >> ReadFromText(known_args.input)
        json_record = csv_records | "Transform" >> (beam.ParDo(CsvToJsonDoFn()).with_outputs('errors', 'success'))
        json_record.success | "Save to file" >> WriteToText(known_args.output)
        json_record.errors | "Save to errors file" >> WriteToText(known_args.errors)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
