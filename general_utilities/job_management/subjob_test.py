import csv

import dxpy
import gzip
from pathlib import Path
from general_utilities.job_management.subjob_utility import SubjobUtility
from general_utilities.association_resources import download_dxfile_by_name, generate_linked_dx_file
from general_utilities.mrc_logger import MRCLogger


LOGGER = MRCLogger(__name__).get_logger()


def test_subjob(tabix_dxfile: dxpy.DXFile):

    tabix_downloaded = download_dxfile_by_name(tabix_dxfile, print_status=True)
    output_tsv = Path('column_stripped.tsv')
    with gzip.open(tabix_downloaded, 'rt') as tabix_open,\
            output_tsv.open('w') as tabix_write:

        tabix_csv = csv.DictWriter(tabix_write, delimiter='\t', fieldnames=['#CHROM','POS','REF','ALT','am_pathogenicity'])
        tabix_csv.writeheader()

        for line in tabix_open:
            if not line.startswith('#'):
                data = line.split('\t')
                write_dict = {'#CHROM': data[0],
                              'POS': data[1],
                              'REF': data[2],
                              'ALT': data[3],
                              'am_pathogenicity': data[8]}
                tabix_csv.writerow(write_dict)
    LOGGER.info('Finished processing tabix file...')

    bgzip_dxlink = generate_linked_dx_file(output_tsv)
    LOGGER.info(f'Uploaded file {bgzip_dxlink}')

    LOGGER.info('Attempting to create subjobs...')
    subjob_launcher = SubjobUtility()
    for chr in range(1,23):
        subjob_launcher.launch_job(function_name='tabix_subjob',
                                   inputs={'input_table': {'$dnanexus_link': bgzip_dxlink.get_id()}, 'chromosome': chr},
                                   outputs=['chromosome', 'subset_tsv'])

    subjob_launcher.submit_queue()

    output_files = []

    for output in subjob_launcher:
        for subjob_output in output:
            if 'field' in subjob_output['$dnanexus_link']:
                link = subjob_output['$dnanexus_link']
                field = link['field']
                field_value = dxpy.DXJob(link['job']).describe()['output'][field]
                LOGGER.info(f'Output for {field}: {field_value}')
                if field == 'subset_tsv':
                    output_files.append(subjob_output)

    return output_files


@dxpy.entry_point('tabix_subjob')
def tabix_subjob(input_table: dict, chromosome: str):
    local_tab = download_dxfile_by_name(input_table, print_status=True)
    output_tab = Path(f'chr{chromosome}.tsv')

    with local_tab.open('r') as local_open,\
        output_tab.open('w') as local_out:

        local_csv = csv.DictReader(local_open, delimiter='\t')
        output_csv = csv.DictWriter(local_out, delimiter='\t', fieldnames=local_csv.fieldnames)

        output_csv.writeheader()

        match_string = f'chr{chromosome}'

        for row in local_csv:
            if row['#CHROM'] == match_string:
                output_csv.writerow(row)

    output = {'chromosome': chromosome, 'subset_tsv': generate_linked_dx_file(output_tab)}
    return output

