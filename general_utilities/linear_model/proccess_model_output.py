import csv
import dxpy
import pysam
import dataclasses
import pandas as pd

from pathlib import Path
from typing import List, Union

from general_utilities.mrc_logger import MRCLogger
from general_utilities.import_utils.import_lib import TarballType
from general_utilities.linear_model.staar_model import STAARModelResult
from general_utilities.linear_model.linear_model import LinearModelResult
from general_utilities.association_resources import define_field_names_from_pandas, bgzip_and_tabix

LOGGER = MRCLogger(__name__).get_logger()

def process_model_outputs(input_models: Union[List[STAARModelResult], List[LinearModelResult]], output_path: Path,
                          tarball_type: TarballType, transcripts_table: pd.DataFrame) -> List[Path]:
    """Process a list of model results into a single output file.

    The 'model results' can currently come from either linear or STAAR models; We have not implemented an Interface for
    to harmonise between these two dataclasses, so additional models would need to conform to a rough specification that
    currently includes only 'mask_name' and 'ENST' fields.

    :param input_models: A list of model results, either STAARModelResult or LinearModelResult
    :param output_path: The path to write the output file to (before bgzip/tabix)
    :param tarball_type: The type of tarball being processed (TarballType Enum)
    :param transcripts_table: A pandas DataFrame containing transcript information.
    :returns: A list with a bgzipped output file of associations at output list index [0]. If TarballType is TarballType.GENOMEWIDE,
        will also sort and provide a tabix index and list index [1].
    """

    outputs = []
    with output_path.open('w') as output_writer:

        # Determine table fieldnames
        fieldnames = []

        # Add gene information, if genomewide mask
        if tarball_type == TarballType.GENOMEWIDE:
            fieldnames.append('ENST')
            fieldnames.extend(transcripts_table.columns)

        # Build the fieldnames programmatically from available data
        model_fieldnames = [linear_field.name for linear_field in dataclasses.fields(type(input_models[0]))]
        if tarball_type == TarballType.GENOMEWIDE:
            model_fieldnames.remove('ENST')  # Remove ENST as we add it above if genomewide
        fieldnames.extend(model_fieldnames)
        mask_maf_fields = define_field_names_from_pandas(input_models[0].mask_name)
        fieldnames[fieldnames.index('mask_name'):fieldnames.index('mask_name')] = mask_maf_fields
        fieldnames.pop(fieldnames.index('mask_name'))

        # Note: We append to a list and then write to the file at once so that we can sort. This should never
        # result in too much in memory as the number of genes is relatively small
        gene_rows = []
        for model in input_models:
            mask_maf_columns = (dict(zip(mask_maf_fields, model.mask_name.split('-'))))

            model_dict = dataclasses.asdict(model)
            model_dict.update(mask_maf_columns)

            if tarball_type == TarballType.GENOMEWIDE:
                try:
                    gene_info = transcripts_table.loc[model.ENST].to_dict()
                except KeyError:
                    # Skip transcripts missing from reference table - they can't be tabix indexed without chrom/start
                    LOGGER.warning(f"Transcript {model.ENST} not found in transcripts table, skipping")
                    continue
            else:
                # Empty dict
                gene_info = {}

            model_dict.update(gene_info)

            gene_rows.append(model_dict)

        output_csv = csv.DictWriter(output_writer, delimiter='\t', fieldnames=fieldnames, extrasaction='ignore')
        output_csv.writeheader()

        # Sort if we are dealing with a genomewide mask
        if tarball_type == TarballType.GENOMEWIDE:
            # Sort by chromosome, start, and end for genomewide masks
            # Use .get() with defaults to handle missing transcript info
            gene_rows = sorted(gene_rows, key=lambda row: (row.get('chrom', 'chrZ'), row.get('start', 0)))
        output_csv.writerows(gene_rows)

    if tarball_type == TarballType.GENOMEWIDE:
        outputs.extend(bgzip_and_tabix(output_path, skip_row=1, sequence_row=2, begin_row=3, end_row=4))
    else:
        output_compressed = output_path.with_suffix('.tsv.gz')
        pysam.tabix_compress(str(output_path.absolute()), str(output_compressed))
        outputs.append(output_compressed)

    return outputs

def merge_glm_staar_runs(output_prefix: str, is_snp_tar: bool = False, is_gene_tar: bool = False) -> List[Path]:

    if is_gene_tar or is_snp_tar:
        if is_gene_tar:
            prefix = 'GENE'
        elif is_snp_tar:
            prefix = 'SNP'
        else:
            raise dxpy.AppError('There should be no way to see this message (error in "merge_glm_staar_runs()")...')

        glm_table = pd.read_csv(output_prefix + '.' + prefix + '.glm.stats.tsv', sep='\t')
        staar_table = pd.read_csv(output_prefix + '.' + prefix + '.STAAR.stats.tsv', sep='\t')

        # We need to pull the ENST value out of the STAAR table 'SNP' variable while trying to avoid any bugs due to
        # file/pheno names
        field_one = staar_table.iloc[0]
        field_one = field_one['SNP'].split("-")
        field_names = []
        if 'ENST' in field_one[0]:
            field_names.append('ENST')
            for i in range(2, len(field_one) + 1):
                field_names.append('var%i' % i)
        else:  # This means we didn't hit on MAF/AC in column [2] and a different naming convention is used...
            raise dxpy.AppError('ENST value is not in the first column of the STAAR table... Error!')

        staar_table[field_names] = staar_table['SNP'].str.split("-", expand=True)

        # Select STAAR columns we need to merge in/match on
        staar_table = staar_table[['ENST', 'pheno_name', 'n_var', 'relatedness.correction', 'staar.O.p', 'staar.SKAT.p',
                                   'staar.burden.p', 'staar.ACAT.p']]

        final_table = glm_table.merge(right=staar_table, on=['ENST', 'pheno_name'])

        gene_path = Path(f'{output_prefix}.{prefix}.STAAR_glm.stats.tsv')
        with gene_path.open('w') as gene_out:
            final_table.to_csv(path_or_buf=gene_out, index=False, sep="\t", na_rep='NA')

        outputs = [gene_path]

    else:
        glm_table = pd.read_csv(f'{output_prefix}.genes.glm.stats.tsv.gz', sep='\t')
        staar_table = pd.read_csv(f'{output_prefix}.genes.STAAR.stats.tsv.gz', sep='\t')

        # Select STAAR columns we need to merge in/match on
        staar_table = staar_table[['ENST', 'MASK', 'MAF', 'pheno_name', 'n_var',
                                   'relatedness_correction', 'p_val_O',
                                   'p_val_SKAT', 'p_val_burden', 'p_val_ACAT']]

        final_table = glm_table.merge(right=staar_table, on=['ENST', 'MASK', 'MAF', 'pheno_name'])

        gene_path = Path(f'{output_prefix}.genes.STAAR_glm.stats.tsv')
        with gene_path.open('w') as gene_out:

            # Sort just in case
            final_table = final_table.sort_values(by=['chrom', 'start', 'end'])
            final_table.to_csv(path_or_buf=gene_out, index=False, sep="\t", na_rep='NA')

        outputs = list(bgzip_and_tabix(gene_path, skip_row=1, sequence_row=2, begin_row=3, end_row=4))

    return outputs
