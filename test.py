import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions

from custom_transforms.joins import Join


def printfn(elem):
    print(elem)


def run(argv=None):
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', type=str, required=True, help='project')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Store the CLI arguments to variables
    project_id = known_args.project

    # Setup the dataflow pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id

    p = beam.Pipeline(options=pipeline_options)

    left_pcol_name = 'p1'
    p1 = p | 'Create source data' >> beam.Create(
        [{'t1_col_A': 1, 't1_col_B': 2, 't1_col_C': 5},
         {'t1_col_A': 1, 't1_col_B': 2, 't1_col_C': 6},
         {'t1_col_A': 1, 't1_col_B': 3, 't1_col_C': 8},
         {'t1_col_A': 1, 't1_col_B': 5, 't1_col_C': 8},
         ])

    right_pcol_name = 'p2'
    p2 = p | 'Create join data' >> beam.Create(
        [{'t2_col_A': 1, 't2_col_B': 2, 't1_col_D': 3},
         {'t2_col_A': 1, 't2_col_B': 3, 't1_col_D': 7},
         {'t2_col_A': 1, 't2_col_B': 6, 't1_col_D': 9},
         ])

    join_keys = {left_pcol_name: ['t1_col_A', 't1_col_B'], right_pcol_name: ['t2_col_A', 't2_col_B']}

    pipelines_dictionary = {left_pcol_name: p1, right_pcol_name: p2}
    test_pipeline = pipelines_dictionary | 'left join' >> Join(left_pcol_name=left_pcol_name, left_pcol=p1,
                                                               right_pcol_name=right_pcol_name, right_pcol=p2,
                                                               join_type='inner', join_keys=join_keys)

    test_pipeline | "print" >> beam.Map(printfn)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
