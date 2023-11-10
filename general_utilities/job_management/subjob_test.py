import csv

import dxpy
import gzip
from pathlib import Path
from general_utilities.job_management.subjob_utility import SubjobUtility
from general_utilities.association_resources import download_dxfile_by_name, bgzip_and_tabix, generate_linked_dx_file
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
        subjob_launcher.launch_job('general_utilities.subjob_test.tabix_subjob',
                                   inputs={'input_table': {'$dnanexus_link': bgzip_dxlink.get_id()}, 'chromosome': chr},
                                   outputs=['chromosome'])

    subjob_launcher.submit_queue()

    for output in subjob_launcher:
        LOGGER.info(f'This chromosome worked: {output}')


@dxpy.entry_point('tabix_subjob')
def run_subjob(input_table: dict, chromosome: str):

    download_dxfile_by_name(input_table, print_status=True)

    output = {'chromosome': chromosome}

    return output
