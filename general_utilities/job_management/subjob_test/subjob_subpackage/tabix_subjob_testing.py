import csv
import dxpy

from pathlib import Path

from general_utilities.import_utils.file_handlers.dnanexus_utilities import download_dxfile_by_name, generate_linked_dx_file


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