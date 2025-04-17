import csv

import dxpy
import gzip
from pathlib import Path

from general_utilities.job_management.subjob_test.subjob_subpackage.tabix_subjob_testing import tabix_subjob
from general_utilities.job_management.subjob_utility import SubjobUtility
from general_utilities.import_utils.file_handlers.dnanexus_utilities import download_dxfile_by_name, generate_linked_dx_file
from general_utilities.mrc_logger import MRCLogger


LOGGER = MRCLogger(__name__).get_logger()


def subjob_testing(tabix_dxfile: dxpy.DXFile, download_on_complete: bool):

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
    subjob_launcher = SubjobUtility(download_on_complete=download_on_complete)
    for chr in range(1, 23):
        subjob_launcher.launch_job(function=tabix_subjob,
                                   inputs={'input_table': {'$dnanexus_link': bgzip_dxlink.get_id()}, 'chromosome': chr},
                                   outputs=['chromosome', 'subset_tsv'],
                                   instance_type='mem1_ssd1_v2_x2',
                                   name=f'{chr}_tabix_test')

    subjob_launcher.submit_queue()

    output_files = []

    for subjob_output in subjob_launcher:
        LOGGER.info(f'Output for subset_tsv: {subjob_output["subset_tsv"]}')
        LOGGER.info(f'Output for chromosome: {subjob_output["chromosome"]}')
        output_files.append(subjob_output['subset_tsv'])

    return output_files
