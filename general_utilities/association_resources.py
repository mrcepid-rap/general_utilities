import csv
import gzip
from pathlib import Path
from typing import List, Union, Tuple, IO, Dict

import dxpy
import pandas as pd
import pandas.core.series
import pysam

from general_utilities.job_management.command_executor import build_default_command_executor
from general_utilities.mrc_logger import MRCLogger

LOGGER = MRCLogger(__name__).get_logger()


def get_chromosomes(is_snp_tar: bool = False, is_gene_tar: bool = False,
                    chromosome: str = None,
                    bgen_dict: Dict = None) -> List[str]:
    """ Generate a list of chromosomes to process

    This method helps to generate a list of data that we want to iterate over. To enable code to iterate over SNP /
    GENE collapsing lists or single chromosomes where an analysis is restricted to a single chromosome, this method
    also can take booleans for whether a SNP list, GENE list, or a single chromosome is being processed.

    This method ultimately will return a list of chromosomes to process given the restrictions provided by the input
    parameters.

    :param is_snp_tar: Is the current analysis processing a collapsed SNP fileset?
    :param is_gene_tar: Is the current analysis processing a collapsed GENE-list fileset?
    :param chromosome: Do we want to analyse a single chromosome?
    :param bgen_dict: A dictionary of bgen files to process
    :return: A list containing chromosomes that we want to analyse
    """

    if is_snp_tar and is_gene_tar:
        raise ValueError('Cannot be both a SNP and a GENE tar!')

    if (is_snp_tar or is_gene_tar) and chromosome is not None:
        LOGGER.warning('Provided a chromosome parameter when SNP/GENE tar are "True". This will have no effect.')

    if is_snp_tar:
        chromosomes = list(['SNP'])

    elif is_gene_tar:
        chromosomes = list(['GENE'])
    else:
        if len(bgen_dict) == 24:
            chromosomes = [f'{chrom}' for chrom in range(1, 23)]  # Is left-closed? (So does 1..22)
            chromosomes.extend(['X'])
            if chromosome:
                if chromosome in chromosomes:
                    LOGGER.info(f'Restricting following analysis to chrom {chromosome}...')
                    chromosomes = [chromosome]
                else:
                    raise ValueError(f'Provided chromosome ({chromosome}) is not 1-22, X. Please try again '
                                     f'(possibly omitting "chr").')
        else:
            chromosomes = list(bgen_dict.keys())

    return chromosomes


def build_transcript_table() -> pd.DataFrame:
    """A wrapper around pd.read_csv to load transcripts.tsv.gz into a pd.DataFrame

    Here we just read the transcripts.tsv.gz file downloaded during ingest_data into a pd.DataFrame. There is some

    :return: A pd.DataFrame representation of all transcripts that can be burden tested for.
    """

    transcripts_table = pd.read_csv('transcripts.tsv.gz', sep="\t", index_col='ENST')
    transcripts_table = transcripts_table[transcripts_table['fail'] == False]
    transcripts_table = transcripts_table.drop(columns=['syn.count', 'fail.cat', 'fail'])
    transcripts_table = transcripts_table[transcripts_table['chrom'] != 'Y']

    # ensure columns are in the expected order:
    transcripts_table = transcripts_table[['chrom', 'start', 'end', 'ENSG', 'MANE', 'transcript_length', 'SYMBOL',
                                           'CANONICAL', 'BIOTYPE', 'cds_length', 'coord', 'manh.pos']]

    # Also generate a table of mean chromosome positions for plotting
    mean_chr_pos = transcripts_table[['chrom', 'manh.pos']].groupby('chrom').mean()

    # Important that this file is sorted by chromosome to ensure proper plotting order...
    chrom_dict = dict(zip([str(x) for x in range(1, 23)] + ["X", "Y"], range(1, 25)))
    mean_chr_pos.sort_index(key=lambda x: x.map(chrom_dict), inplace=True)
    mean_chr_pos.to_csv('mean_chr_pos.tsv', sep='\t')

    return transcripts_table


def get_gene_id(gene_id: str, transcripts_table: pandas.DataFrame) -> pandas.core.series.Series:
    """Extract the information for a gene as a pandas.series

    This function will query the transcripts table (as loaded by :func:`build_transcript_table`) and extract a single
    gene based on either the symbol (using `==` on the Symbol column) or ENST id (using index `.loc`). If neither is found,
    the method will report an error.

    :param gene_id: A string that is either a gene symbol or a valid ENST ID (most likely a MANE transcript)
    :param transcripts_table: A pandas.DataFrame loaded in the format specified by :func:`build_transcript_table`
    :return: A `pandas.Series` object with information about the queried gene as represented by the columns present in
        transcripts_table.
    """

    # If we find 'ENST' at the _start_ of the gene ID provided, then assume we need to query pandas.index.
    if gene_id.startswith('ENST'):
        LOGGER.info("gene_id – " + gene_id + " – looks like an ENST value... validating...")
        try:
            gene_info = transcripts_table.loc[gene_id]

            # If we get a pd.DataFrame back, that means that we found more than one gene for a single ENST. This
            # should be _very_ rare
            if type(gene_info) is pd.DataFrame:
                raise ValueError(f'Found {len(gene_info)} ENST IDs ({",".join(gene_info["SYMBOL"].to_list())} for '
                                 f'ENST ID {gene_id}... Please re-run using SYMBOL to ensure consistent results...')
            LOGGER.info(f'Found one matching ENST ({gene_id} - {gene_info["coord"]})... proceeding...')
        except KeyError:
            raise KeyError(f'Did not find a transcript with ENST value {gene_id}... terminating...')

    # Otherwise see if we can find a SINGLE gene with a given SYMBOL in the table using ==
    else:
        LOGGER.warning("gene_id – " + gene_id + " – does not look like an ENST value, searching for symbol instead...")
        found_rows = transcripts_table[transcripts_table['SYMBOL'] == gene_id]
        if len(found_rows) == 1:
            found_enst = found_rows.index[0]
            gene_info = transcripts_table.loc[found_enst]
            LOGGER.info(f'Found one matching ENST ({found_enst} - {gene_info["coord"]}) for SYMBOL {gene_id}... '
                        f'proceeding...')
        elif len(found_rows) > 1:
            # I'm fairly certain this case is impossible with the current (July 2022; VEP 107 / UKB WES 470k) release
            raise ValueError(f'Found {len(found_rows)} ENST IDs ({",".join(found_rows.index.to_list())} for SYMBOL '
                             f'{gene_id}... Please re-run using exact ENST to ensure consistent results...')
        else:
            raise ValueError(f'Did not find an associated ENST ID for SYMBOL {gene_id}... '
                             f'Please re-run after checking SYMBOL/ENST used...')

    return gene_info


def process_snp_or_gene_tar(is_snp_tar, is_gene_tar, tarball_prefix) -> tuple:
    if is_snp_tar:
        LOGGER.info("Running in SNP mode...")
        file_prefix = 'SNP'
        gene_id = 'ENST00000000000'
    elif is_gene_tar:
        LOGGER.info("Running in GENE mode...")
        file_prefix = 'GENE'
        gene_id = 'ENST99999999999'
    else:
        raise dxpy.AppError('There is no way you should see this error (process_snp_or_gene_tar)')

    # Get the chromosomes that are represented in the SNP/GENE tarball
    # This variants_table file will ONLY have chromosomes in it represented by a provided SNP/GENE list
    chromosomes = set()
    sparse_matrix = csv.DictReader(
        open(tarball_prefix + '.' + file_prefix + '.variants_table.STAAR.tsv', 'r'),
        delimiter="\t",
        quoting=csv.QUOTE_NONE)

    for row in sparse_matrix:
        chromosomes.add(str(row['chrom']))

    # And filter the relevant SAIGE file to just the individuals we want so we can get actual MAC
    cmd_executor = build_default_command_executor()
    cmd = f'bcftools view --threads 4 -S /test/SAMPLES_Include.bcf.txt -Ob -o /test/' \
          f'{tarball_prefix}.{file_prefix}.saige_input.bcf /test/' \
          f'{tarball_prefix}.{file_prefix}.SAIGE.bcf'
    cmd_executor.run_cmd_on_docker(cmd)

    # Build a fake gene_info that can feed into the other functions in this class
    gene_info = pd.Series({'chrom': file_prefix, 'SYMBOL': file_prefix})
    gene_info.name = gene_id

    return gene_info, chromosomes


# These two methods help the different tools in defining the correct field names to include in outputs
def define_field_names_from_pandas(field_one: pd.Series) -> List[str]:
    # Test what columns we have in the 'SNP' field, so we can name them...
    field_one = field_one['SNP'].split("-")
    field_names = ['ENST']
    if len(field_one) == 2:  # This is the bare minimum, always name first column ENST, and second column 'var1'
        field_names.append('var1')
    elif len(field_one) == 3:  # This could be the standard naming format... check that column [2] is MAF/AC
        if 'MAF' in field_one[2] or 'AC' in field_one[2]:
            field_names.extend(['MASK', 'MAF'])
        else:  # This means we didn't hit on MAF in column [2] and a different naming convention is used...
            field_names.extend(['var1', 'var2'])
    else:
        for i in range(2, len(field_one) + 1):
            field_names.append('var%i' % i)

    return field_names


def define_field_names_from_tarball_prefix(tarball_prefix: str, variant_table: pd.DataFrame) -> pd.DataFrame:
    tarball_prefix_split = tarball_prefix.split("-")
    if len(tarball_prefix_split) == 2:  # This could be the standard naming format. Check that column [1] is MAF/AC
        if 'MAF' in tarball_prefix_split[1] or 'AC' in tarball_prefix_split[1]:
            field_names = ['MASK', 'MAF']
            variant_table[field_names] = tarball_prefix_split
        else:  # This means we didn't hit on MAF/AC in column [2] and a different naming convention is used...
            field_names = ['var1', 'var2']
            variant_table[field_names] = tarball_prefix_split
    else:
        for i in range(1, len(tarball_prefix_split) + 1):
            field_name = 'var%i' % i
            variant_table[field_name] = tarball_prefix_split[i - 1]

    return variant_table


# Helper function to decide what covariates are included in the various REGENIE commands
def define_covariate_string(found_quantitative_covariates: List[str], found_categorical_covariates: List[str],
                            is_binary: bool, add_array: bool, ignore_base: bool) -> str:
    quant_covars = [] if ignore_base else ['PC{1:10}', 'age', 'age_squared', 'sex']
    quant_covars.extend(found_quantitative_covariates)

    cat_covars = [] if ignore_base else (['wes_batch', 'array_batch'] if add_array else ['wes_batch'])
    cat_covars.extend(found_categorical_covariates)

    covar_string = ''
    if len(quant_covars) > 0:
        quant_covars_join = ','.join(quant_covars)
        covar_string += f'--covarColList {quant_covars_join} '

    if len(cat_covars) > 0:
        cat_covars_join = ','.join(cat_covars)
        covar_string += f'--catCovarList {cat_covars_join} '

    if is_binary:
        covar_string += '--bt --firth --approx '

    return covar_string


def gt_to_float(gt: str) -> float:
    """Convert a VCF genotype-like string into a float

    This method is a slightly incorrect as it converts a string genotype into a 0,2 range, but as a float instead of
    as the expected int. This is because:

    1. Integers cannot by NULL / NA in python.

    2. I use pandas to handle most of my genotype information, which can only use floats as a nullable datatype Thus
    I convert to a float and then cast to an integer in string format if required by the output that uses this method.

    :param gt: A VCF genotype-like string (e.g., 0/0)
    :return: A float representation of the string between 0 - 2
    """
    if gt == '0/0':
        return 0
    elif gt == '0/1':
        return 1
    elif gt == '1/1':
        return 2
    else:
        return float('NaN')


# This function will locate an associated tbi/csi index:
def find_index(parent_file: Union[dxpy.DXFile, dict], index_suffix: str) -> dxpy.DXFile:
    if type(parent_file) == dict:
        parent_file = dxpy.DXFile(parent_file['$dnanexus_link'])

    # Describe the file to get attributes:
    file_description = parent_file.describe(fields={'folder': True, 'name': True, 'project': True})

    # First set the likely details of the corresponding index:
    project_id = dxpy.PROJECT_CONTEXT_ID
    index_folder = file_description['folder']
    index_name = file_description['name'] + '.' + index_suffix

    # Run a dxpy query.
    # This will fail if no or MULTIPLE indices are found
    index_object = dxpy.find_one_data_object(more_ok=False, classname='file', project=project_id,
                                             folder=index_folder,
                                             name=index_name, name_mode='exact')

    # Set a dxfile of the index itself:
    found_index = dxpy.DXFile(dxid=index_object['id'], project=index_object['project'])

    return found_index


def bgzip_and_tabix(file_path: Path, comment_char: str = ' ', skip_row: int = 0,
                    sequence_row: int = 1, begin_row: int = 2, end_row: int = 3) -> Tuple[Path, Path]:
    """
    Compress a file using bgzip and create a tabix index.

    This function uses pysam to compress a file with bgzip and create a corresponding tabix index.
    The index parameters can be customized to match the file format.

    NOTE: Make sure to specify the parameters (column indices) when running this. The default column numbers are in BED
    format. Please modify the command if using any non-BED format.

    :param file_path: Path to the input file to be compressed and indexed.
    :param comment_char: Comment character to identify header lines (default: '#').
    :param skip_row: Number of header lines to skip in the index (default: 0).
    :param sequence_row: 1-based column number containing sequence names (default: 1).
    :param begin_row: 1-based column number containing start positions (default: 2).
    :param end_row: 1-based column number containing end positions (default: 3).
    :return: A tuple containing paths to the compressed file (.gz) and its index (.tbi).
    """

    # Fix header if it starts with a tab
    with file_path.open("r") as f:
        lines = f.readlines()

    if lines and lines[0].startswith('\t'):
        # Remove leading tab from header only
        lines[0] = lines[0].lstrip('\t')
        with file_path.open("w") as f:
            f.writelines(lines)

    # Compress using pysam
    outfile_compress = f'{file_path}.gz'
    pysam.tabix_compress(str(file_path.absolute()), outfile_compress)

    try:
        # Run indexing via pysam, and incorporate comment character if requested
        pysam.tabix_index(outfile_compress, seq_col=sequence_row - 1, start_col=begin_row - 1, end_col=end_row - 1,
                          meta_char=comment_char, line_skip=skip_row)
    except Exception as e:
        LOGGER.error(f"Failed to index file {outfile_compress}: {e}. Check the bgzip_and_tabix command in "
                     f"general_utilities - it's likely that the header settings need to be adjusted for your "
                     f"file format")

    return Path(outfile_compress), Path(f'{outfile_compress}.tbi')


def get_include_sample_ids() -> List[str]:
    """Get the sample IDs from the SAMPLES_Include.txt file and return them as a List[str]

    :return: A List[str] of all sample IDs in the SAMPLES_Include.txt file created by
        general_utilities.import_utils.module_loader.ingest_data.py
    """

    with Path('SAMPLES_Include.txt').open('r') as include:
        samples_list = []
        for samp in include:
            samp_id = samp.rstrip().split()[0]  # Have to do this as the file follows plink IID\tFID convention.
            samples_list.append(samp_id)

    return samples_list


def check_gzipped(file_path: Path) -> IO:
    """Check if a file is gzipped and return the appropriate file handle via the appropriate open method'

    :param file_path: A PosixPath representation of a possible .tsv / .txt file
    :return: An IO handle for that file with either gzip or path open methods
    """
    if file_path.suffixes[-1] == '.gz':
        return gzip.open(file_path, 'rt')
    else:
        return file_path.open('r')


def fix_plink_bgen_sample_sex(sample_file: Path) -> Path:
    """A simple wrapper to write a fake sex to a .bgen sample file to allow processing via plink2

    plink2 has introduced an issue with chrX processing. Briefly, plink2 now requires sex to be encoded in the sample
    file so that hemizygous state in males can be correctly encoded. We cannot use this, as BOLT does not allow for
    hemizygous ploidy when doing association tests with .bgen inputs. Thus, we encode all individuals in a given .sample
    file to be female. One must also ensure that the --split-par hg38 flag is used when running plink2 to ensure that
    these settings work.

    :param sample_file: A .sample file for .bgen format genotypes
    :return: A sample file with fake sex. We DO NOT replace the old sample file!
    """

    fixed_sample = sample_file.with_suffix('.fix.sample')
    with sample_file.open('r') as sample_file, \
            fixed_sample.open('w') as fix_sample_file:

        sample_reader = csv.DictReader(sample_file, delimiter=' ')
        fix_sample_writer = csv.DictWriter(fix_sample_file, delimiter=' ', fieldnames=sample_reader.fieldnames)
        fix_sample_writer.writeheader()

        # sample-v1 and sample-v2 have slightly different headers
        id_header = 'ID_1' if 'ID_1' in sample_reader.fieldnames else 'ID'

        # Write sex to 'fix' sample file
        for sample in sample_reader:
            if sample[id_header] == '0':
                fix_sample_writer.writerow(sample)
            else:
                sample['sex'] = '2'
                fix_sample_writer.writerow(sample)

    return fixed_sample


def replace_multi_suffix(original_path: Path, new_suffix: str) -> Path:
    """A helper function to replace a path on a file with multiple suffixes (e.g., .tsv.gz)

    This function just loops through the path and recursively removes the string after '.'. Once there are no more
    full stops it then adds the requested :param: new_suffix.

    :param original_path: The original filepath
    :param new_suffix: The new suffix to add
    :return: A Pathlike to the new file
    """

    while original_path.suffix:
        original_path = original_path.with_suffix('')

    return original_path.with_suffix(new_suffix)
