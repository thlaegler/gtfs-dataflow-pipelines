#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A minimalist word-counting workflow that counts words in Shakespeare.

This is the first in a series of successively more detailed 'word count'
examples.

Next, see the wordcount pipeline, then the wordcount_debugging pipeline, for
more detailed examples that introduce additional concepts.

Concepts:

1. Reading data from text files
2. Specifying 'inline' transforms
3. Counting a PCollection
4. Writing data to Cloud Storage as text files

To execute this pipeline locally, first edit the code to specify the output
location. Output location could be a local file path or an output prefix
on GCS. (Only update the output location marked with the first CHANGE comment.)

To execute this pipeline remotely, first edit the code to set your project ID,
runner type, the staging location, the temp location, and the output location.
The specified GCS bucket(s) must already exist. (Update all the places marked
with a CHANGE comment.)

Then, run the pipeline as described in the README. It will be deployed and run
using the Google Cloud Dataflow Service. No args are required to run the
pipeline. You can see the results in your output bucket in the GCS browser.
"""

from __future__ import absolute_import

import argparse
import logging
import re
import csv
from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import WriteToText
from beam_nuggets.io import relational_db
from apache_beam import (coders, io, transforms)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='$GTFS_BUCKET/at/20190429120000/at.zip',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default='$GTFS_BUCKET/_DATAFLOW/output',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--runner=DataflowRunner',
        # CHANGE 3/5: Your project ID is required in order to run your pipeline on
        # the Google Cloud Dataflow Service.
        '--project=$PROJECT_ID',
        # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=$GTFS_BUCKET/_DATAFLOW/staging',
        # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=$GTFS_BUCKET/_DATAFLOW/temp',
        '--job_name=gtfs-import-job',
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection.
        # zip = p | io.Read("Reading GTFS zip file", beam.io.TextFileSource(known_args.input, coder=coders.BytesCoder()))
        csv_files = p | 'ReadFromText' >> beam.io.ReadFromText(
            known_args.input,
            compression_type=beam.io.filesystem.CompressionTypes.GZIP)
        unzipped = zip | transforms.Map(lambda x: x | WriteToText(known_args.output))
        vids = (p | 'Read input' >> beam.io.ReadFromText(known_args.input)
                | 'Parse input' >> beam.Map(lambda line: csv.reader([line]).next()))
                # | 'Run DeepMeerkat' >> beam.ParDo(PredictDoFn(pipeline_args)))

        months = p | "Reading month records" >> beam.Create([
            {'name': 'Jan', 'num': 1},
            {'name': 'Feb', 'num': 2},
        ])
        source_config = relational_db.SourceConfiguration(
            drivername='postgresql+pg8000',
            host='35.189.51.235',
            port=5432,
            username='postgres',
            password='7f512a41a3506989d1233017f624b35c0fa70bae',
            database='at',
            create_if_missing=True,
        )
        table_config = relational_db.TableConfiguration(
            name='months',
            create_if_missing=True
        )
        months | 'Writing to DB' >> relational_db.Write(
            source_config=source_config,
            table_config=table_config
        )

        def per_column_average(rows, ignore_elms=[ID_INDEX]):
            return [sum([row[idx] if idx not in ignore_elms else 0
                         for row in rows]) / len(row[0])
                    for idx, _ in enumerate(rows[0])]

        keyed_averaged_elm = (months
                              | beam.Map(lambda x: (x[ID_INDEX], x))
                              | beam.GroupByKey()
                              | beam.Map(lambda x: (x[0], per_column_average(rows))

        # Count the occurrences of each word.
        counts = (
                unzipped
                | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                              .with_output_types(unicode))
                | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
                | 'GroupAndSum' >> beam.CombinePerKey(sum))

        # Format the counts into a PCollection of strings.
        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)

        output = counts | 'Format' >> beam.Map(format_result)

        # Write the output using a "Write" transform that has side effects.
        # pylint: disable=expression-not-assigned
        output | WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
