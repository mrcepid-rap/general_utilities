import csv
import sys
import dxpy
import subprocess
import pandas as pd
import pandas.core.series

from pathlib import Path
from typing import List, Union, Tuple

from general_utilities.mrc_logger import MRCLogger
from general_utilities.job_management.command_executor import build_default_command_executor

LOGGER = MRCLogger(__name__).get_logger()


# TODO: Convert this to a class that sets the DockerImage at startup by RunAssociationTesting/plugin AbstractClass(es)?
def run_cmd(cmd: str, is_docker: bool = False, docker_image: str = None,
            data_dir: str = '/home/dnanexus/', docker_mounts: List = None,
            stdout_file: Union[str, Path] = None, print_cmd: bool = False, livestream_out: bool = False,
            dry_run: bool = False) -> int:

    """Run a command in the shell either with / without Docker

    This function runs a command on an instance via the subprocess module, either with or without calling the Docker
    instance we downloaded; by default, commands are not run via Docker, but can be changed by setting is_docker =
    True. However, if running with Docker, a docker image **MUST** be provided or this method will fail. Docker
    images are run in headless mode, which cannot be modified. Also, by default, standard out is not saved,
    but can be modified with the 'stdout_file' parameter. print_cmd, livestream_out, and/or dry_run are for internal
    debugging purposes when testing new code. All options other than `cmd` are optional.

    :param cmd: The command to be run
    :param is_docker: Run the command via a docker image (image must be provided via `docker_image`)
    :param docker_image: Docker image on some repository to run the command via. This image does not necessarily have
        to be on the image, but if in a non public repository (e.g., AWS ECR) this will cause the command to fail.
    :param data_dir: Mount location where data to be processed in Docker is located. By default is the home directory
        on an DNANexus AWS instance (`/home/dnanexus/`). This directory will be mounted as `/test/` inside of the Docker
        image.
    :param docker_mounts: Additional Docker mounts to attach to this process via the `-v` commandline argument to
        Docker. See the documentation for Docker for more information.
    :param stdout_file: Capture stdout from the process into the given file
    :param print_cmd: Print `cmd` but still run the command (as opposed to dry_run). For debug purposes only.
    :param livestream_out: Livestream the output from the requested process. For debug purposes only.
    :param dry_run: Print `cmd` and exit without running. For debug purposes only.
    :returns: The exit code of the underlying process
    """

    # -v here mounts a local directory on an instance (in this case the home dir) to a directory internal to the
    # Docker instance named /test/. This allows us to run commands on files stored on the AWS instance within Docker.
    # Multiple mounts can be added (via docker_mounts) to enable this code to find other specialised files (e.g.,
    # some R scripts included in the associationtesting suite).
    if is_docker:
        if docker_image is None:
            raise dxpy.AppError('Requested to run via docker without providing a Docker image!')

        if docker_mounts is None:
            docker_mount_string = ''
        else:
            docker_mount_string = ' '.join([f'-v {mount}' for mount in docker_mounts])
        cmd = f'docker run ' \
              f'-v {data_dir}:/test ' \
              f'{docker_mount_string} ' \
              f'{docker_image} {cmd}'

    if dry_run:
        LOGGER.info(cmd)
        return 0
    else:
        if print_cmd:
            LOGGER.info(cmd)

        # Standard python calling external commands protocol
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if livestream_out:

            for line in iter(proc.stdout.readline, b""):
                LOGGER.info(f'SUBPROCESS STDOUT: {bytes.decode(line).rstrip()}')

            proc.wait()  # Make sure the process has actually finished...
            if proc.returncode != 0:
                LOGGER.error("The following cmd failed:")
                LOGGER.error(cmd)
                LOGGER.error("STDERR follows\n")
                for line in iter(proc.stderr.readline, b""):
                    sys.stdout.buffer.write(line)
                raise dxpy.AppError("Failed to run properly...")

        else:
            stdout, stderr = proc.communicate()
            if stdout_file is not None:
                with Path(stdout_file).open('w') as stdout_writer:
                    stdout_writer.write(stdout.decode('utf-8'))
                stdout_writer.close()

            # If the command doesn't work, print the error stream and close the AWS instance out with 'dxpy.AppError'
            if proc.returncode != 0:
                LOGGER.error("The following cmd failed:")
                LOGGER.error(cmd)
                LOGGER.error("STDOUT follows")
                LOGGER.error(stdout.decode('utf-8'))
                LOGGER.error("STDERR follows")
                LOGGER.error(stderr.decode('utf-8'))
                raise RuntimeError(f'run_cmd() failed to run requested job properly')

        return proc.returncode


def get_chromosomes(is_snp_tar: bool = False, is_gene_tar: bool = False, chromosome: str = None) -> List[str]:
    """ Generate a list of chromosomes to process

    This method helps to generate a list of data that we want to iterate over. To enable code to iterate over SNP /
    GENE collapsing lists or single chromosomes where an analysis is restricted to a single chromosome, this method
    also can take booleans for whether a SNP list, GENE list, or a single chromosome is being processed.

    This method ultimately will return a list of chromosomes to process given the restrictions provided by the input
    parameters.

    :param is_snp_tar: Is the current analysis processing a collapsed SNP fileset?
    :param is_gene_tar: Is the current analysis processing a collapsed GENE-list fileset?
    :param chromosome: Do we want to analyse a single chromosome?
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
        chromosomes = [f'{chrom}' for chrom in range(1, 23)]  # Is left-closed? (So does 1..22)
        chromosomes.extend(['X'])
        if chromosome:
            if chromosome in chromosomes:
                LOGGER.info(f'Restricting following analysis to chrom {chromosome}...')
                chromosomes = [chromosome]
            else:
                raise ValueError(f'Provided chromosome ({chromosome}) is not 1-22, X. Please try again '
                                 f'(possibly omitting "chr").')

    return chromosomes


def generate_linked_dx_file(file: Union[str, Path]) -> dxpy.DXFile:
    """A helper function to upload a local file to the DNANexus platform and then remove it from the instance.

     A simple wrapper artound :func:`dxpy.upload_local_file` with additional functionality to remove the file from
     the local instance storage system.

    :param file: Either a str or Path representation of the file to upload.
    :return: A :func:`dxpy.DXFile` instance of the remote file.
    """

    if type(file) == str:
        linked_file = dxpy.upload_local_file(filename=file)
        Path(file).unlink()
    else:
        linked_file = dxpy.upload_local_file(file=file.open('rb'))
        file.unlink()
    return linked_file


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


def download_dxfile_by_name(file: Union[dict, str, dxpy.DXFile], project_id: str = None,
                            print_status: bool = True) -> Path:
    """Download a dxfile and downloads to the file 'name' as given by dxfile.describe()

    This method can take either:

    1. A DNANexus link (i.e., in the style provided to :func:`main` at startup)

    2. A dict from a 'find_objects()' call (has keys of 'id' and 'project')

    3. A string representation of a DNANexus file (e.g., file-12345...)

    4. A DNANexus file object from dxpy.DXFile

    And will then download this file to the local environment using the remote name of the file.

    :param file: A DNANexus link / file-ID string, or dxpy.DXFile object to download
    :param project_id: Optional project ID of the file to be downloaded. Only required if accessing bulk data or
        downloading a file from another project.
    :param print_status: Should this method print a message indicating that the provided file is being downloaded?
    :return: A Path pointing to the file on the local filesystem
    """
    if type(file) == dict:
        if 'id' in file:
            file = dxpy.DXFile(dxid=file['id'], project=file['project'])
        else:
            file = dxpy.DXFile(file)
    elif type(file) == str:
        file = dxpy.DXFile(file)

    curr_filename = file.describe()['name']

    if print_status:
        LOGGER.info(f'Downloading file {curr_filename} ({file.get_id()})')
    dxpy.download_dxfile(file.get_id(), curr_filename, project=project_id)

    return Path(curr_filename)


# This function will locate an associated tbi/csi index:
def find_index(parent_file: dxpy.DXFile, index_suffix: str) -> dxpy.DXFile:

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


def bgzip_and_tabix(file_path: Path, comment_char: str = None, skip_row: int = None,
                    sequence_row: int = 1, begin_row: int = 2, end_row: int = 3) -> Tuple[Path, Path]:
    """BGZIP and TABIX a provided file path

    This is a wrapper for bgzip and tabix. In its simplest form will take a filepath and run bgzip and tabix,
    with default sequence, begin, and end columns. The user can modify default column specs using parameters and also
    provide a comment character to set a header line in tabix.

    :param file_path: A Pathlike to a file on this platform.
    :param comment_char: A comment character to skip. MUST be a single character. Defaults to 'None'
    :param skip_row: Number of lines at the beginning of the file to skip using tabix -S parameter
    :param sequence_row: Row number (in base 1) of the chromosome / sequence name column
    :param begin_row: Row number (in base 1) of the start coordinate column
    :param end_row: Row number (in base 1) of the end coordinate column. This value can be the same as begin row for
        files without an end coordinate but cannot be omitted.
    :return: A Tuple consisting of the bgziped file and it's corresponding tabix index
    """

    # Run bgzip
    cmd_executor = build_default_command_executor()
    bgzip_cmd = f'bgzip /test/{file_path}'
    cmd_executor.run_cmd_on_docker(bgzip_cmd)

    # Run tabix, and incorporate comment character if requested
    tabix_cmd = 'tabix '
    if comment_char:
        tabix_cmd += f'-c {comment_char} '
    if skip_row:
        tabix_cmd += f'-S {skip_row} '
    tabix_cmd += f'-s {sequence_row} -b {begin_row} -e {end_row} /test/{file_path}.gz'
    cmd_executor.run_cmd_on_docker(tabix_cmd)
    
    return Path(f'{file_path}.gz'), Path(f'{file_path}.gz.tbi')


def get_sample_count() -> int:
    # Need to define separate min/max MAC files for REGENIE as it defines them slightly differently from BOLT:
    # First we need the number of individuals that are being processed:
    with open('SAMPLES_Include.txt') as sample_file:
        n_samples = 0
        for _ in sample_file:
            n_samples += 1
        sample_file.close()

    return n_samples
