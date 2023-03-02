import os
import csv
import sys
from pathlib import Path
from typing import List, Union, TypedDict

import dxpy
import logging
import subprocess
import pandas as pd
import pandas.core.series

from general_utilities.mrc_logger import MRCLogger


# TODO: Convert this to a class that sets the DockerImage at startup by RunAssociationTesting/plugin AbstractClass(es)?
def run_cmd(cmd: str, is_docker: bool = False, docker_image: str = None,
            data_dir: str = '/home/dnanexus/', docker_mounts: List = None,
            stdout_file: Union[str, Path] = None, print_cmd: bool = False, livestream_out: bool = False,
            dry_run: bool = False) -> None:

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
    :return: None
    """

    # This is required if running on DNA Nexus to propogate messages from subprocesses to their custom
    # event-reporter. So, if we are running inside a DNANexus job, we set the logger to the dxpy handler. Otherwise,
    # just use a default logger.
    if 'DX_JOB_ID' in os.environ:
        logger = MRCLogger(__name__).get_logger()
    else:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()

    # -v here mounts a local directory on an instance (in this case the home dir) to a directory internal to the
    # Docker instance named /test/. This allows us to run commands on files stored on the AWS instance within Docker.
    # This looks slightly different from other versions of this command I have written as I needed to write a custom
    # R script to run STAAR. That means we have multiple mounts here to enable this code to find the script.
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
        logger.info(cmd)
    else:
        if print_cmd:
            logger.info(cmd)

        # Standard python calling external commands protocol
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if livestream_out:

            for line in iter(proc.stdout.readline, b""):
                logging.info(f'SUBPROCESS STDOUT: {bytes.decode(line).rstrip()}')

            proc.wait()  # Make sure the process has actually finished...
            if proc.returncode != 0:
                logging.error("The following cmd failed:")
                logging.error(cmd)
                logging.error("STDERR follows\n")
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
                logger.error("The following cmd failed:")
                logger.error(cmd)
                logger.error("STDOUT follows")
                logger.error(stdout.decode('utf-8'))
                logger.error("STDERR follows")
                logger.error(stderr.decode('utf-8'))
                raise dxpy.AppError("Failed to run properly...")


# This is to generate a global CHROMOSOMES variable for parallelisation
def get_chromosomes(is_snp_tar: bool = False, is_gene_tar: bool = False, chromosome: str = None) -> List[str]:

    if is_snp_tar:
        chromosomes = list(['SNP'])
    elif is_gene_tar:
        chromosomes = list(['GENE'])
    else:
        chromosomes = [f'{chrom}' for chrom in range(1, 23)]  # Is left-closed? (So does 1..22)
        chromosomes.extend(['X'])
        if chromosome:
            if chromosome in chromosomes:
                print(f'Restricting following analysis to chrom{chromosome}...')
                chromosomes = [chromosome]
            else:
                raise dxpy.AppError(f'Provided chromosome ({chromosome}) is not 1-22, X. Please try again (possibly omitting "chr").')

    return chromosomes


# This is a helper function to upload a local file and then remove it from the instance.
# This is different than other applets I have written since CADD takes up so much space.
# I don't want to have to use a massive instance costing lots of £s!
def generate_linked_dx_file(file: Union[str, Path]) -> dxpy.DXFile:

    if type(file) == str:
        linked_file = dxpy.upload_local_file(filename=file)
        Path(file).unlink()
    else:
        linked_file = dxpy.upload_local_file(file=file)
        file.unlink()
    return linked_file


# A TypedDict holding information about each chromosome's available genetic data
class BGENInformation(TypedDict):
    index: dxpy.DXFile
    sample: dxpy.DXFile
    bgen: dxpy.DXFile
    vep: dxpy.DXFile


# This downloads and process a bgen file when requested
def process_bgen_file(chrom_bgen_index: BGENInformation, chromosome: str, download_only: bool = False) -> None:

    # First we have to download the actual data
    bgen_index = chrom_bgen_index['index']
    bgen_sample = chrom_bgen_index['sample']
    bgen = chrom_bgen_index['bgen']
    vep = chrom_bgen_index['vep']
    dxpy.download_dxfile(bgen_index.get_id(), f'filtered_bgen/{chromosome}.filtered.bgen.bgi')
    dxpy.download_dxfile(bgen_sample.get_id(), f'filtered_bgen/{chromosome}.filtered.sample')
    dxpy.download_dxfile(bgen.get_id(), f'filtered_bgen/{chromosome}.filtered.bgen')
    dxpy.download_dxfile(vep.get_id(), f'filtered_bgen/{chromosome}.filtered.vep.tsv.gz')

    # And then do filtering if requested
    if not download_only:
        cmd = f'plink2 --threads 4 --bgen /test/filtered_bgen/{chromosome}.filtered.bgen "ref-last" ' \
              f'--sample /test/filtered_bgen/{chromosome}.filtered.sample ' \
              f'--export bgen-1.2 "bits="8 ' \
              f'--out /test/{chromosome}.markers ' \
              f'--keep /test/SAMPLES_Include.txt'
        run_cmd(cmd, is_docker=True, docker_image='egardner413/mrcepid-burdentesting')

        # And index the file
        cmd = f'bgenix -index -g /test/{chromosome}.markers.bgen'
        run_cmd(cmd, is_docker=True, docker_image='egardner413/mrcepid-burdentesting')

        # The sample file output by plink2 is a disaster, so fix it here:
        os.rename(chromosome + '.markers.sample', chromosome + '.old')
        with open(chromosome + '.old', 'r') as samp_file:
            fixed_samp_bolt = open(chromosome + '.markers.bolt.sample', 'w')
            for line in samp_file:
                line = line.rstrip().split(" ")
                if line[0] == 'ID_1':
                    fixed_samp_bolt.write('ID_1 ID_2 missing sex\n')
                elif line[3] == 'D':
                    fixed_samp_bolt.write('0 0 0 D\n')
                else:
                    fixed_samp_bolt.write(f'{line[1]} {line[1]} 0 NA\n')
            samp_file.close()
            fixed_samp_bolt.close()

    else:
        # REGENIE cannot use the bgen v2 sample file, fix here:
        os.rename(f'filtered_bgen/{chromosome}.filtered.sample', f'{chromosome}.old')
        with open(f'{chromosome}.old', 'r') as samp_file:
            fixed_samp_bolt = open(f'{chromosome}.markers.bolt.sample', 'w')
            for line in samp_file:
                line = line.rstrip().split(" ")
                if line[0] == 'ID':
                    fixed_samp_bolt.write('ID_1 ID_2 missing sex\n')
                elif line[2] == 'D':
                    fixed_samp_bolt.write('0 0 0 D\n')
                else:
                    fixed_samp_bolt.write(f'{line[0]} {line[0]} 0 NA\n')
            samp_file.close()
            fixed_samp_bolt.close()


# Build the pandas DataFrame of transcripts
def build_transcript_table() -> pandas.DataFrame:

    transcripts_table = pd.read_csv('transcripts.tsv.gz', sep="\t", index_col='ENST')
    transcripts_table = transcripts_table[transcripts_table['fail'] == False]
    transcripts_table = transcripts_table.drop(columns=['syn.count', 'fail.cat', 'fail'])
    return transcripts_table


def get_gene_id(gene_id: str, transcripts_table: pandas.DataFrame) -> pandas.core.series.Series:

    if 'ENST' in gene_id:
        print("gene_id – " + gene_id + " – looks like an ENST value... validating...")
        try:
            gene_info = transcripts_table.loc[gene_id]
            print(f'Found one matching ENST ({gene_id} - {gene_info["coord"]})... proceeding...')
        except KeyError:
            raise dxpy.AppError(f'Did not find a transcript with ENST value {gene_id}... terminating...')
    else:
        print("gene_id – " + gene_id + " – does not look like an ENST value, searching for symbol instead...")
        found_rows = transcripts_table[transcripts_table['SYMBOL'] == gene_id]
        if len(found_rows) == 1:
            found_enst = found_rows.index[0]
            gene_info = transcripts_table.loc[found_enst]
            print(f'Found one matching ENST ({found_enst} - {gene_info["coord"]}) for SYMBOL {gene_id}... '
                  f'proceeding...')
        elif len(found_rows) > 1:
            raise dxpy.AppError(f'Found {len(found_rows)} ENST IDs ({",".join(found_rows.index.to_list())} for SYMBOL '
                                f'{gene_id}... Please re-run using exact ENST to ensure consistent results...')
        else:
            raise dxpy.AppError(f'Did not find an associated ENST ID for SYMBOL {gene_id}... '
                                f'Please re-run after checking SYMBOL/ENST used...')

    return gene_info


def process_snp_or_gene_tar(is_snp_tar, is_gene_tar, tarball_prefix) -> tuple:

    if is_snp_tar:
        print("Running in SNP mode...")
        file_prefix = 'SNP'
        gene_id = 'ENST00000000000'
    elif is_gene_tar:
        print("Running in GENE mode...")
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
    cmd = f'bcftools view --threads 4 -S /test/SAMPLES_Include.txt -Ob -o /test/' \
          f'{tarball_prefix}.{file_prefix}.saige_input.bcf /test/' \
          f'{tarball_prefix}.{file_prefix}.SAIGE.bcf'
    run_cmd(cmd, is_docker=True, docker_image='egardner413/mrcepid-burdentesting')

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
                            is_binary: bool) -> str:

    suffix = ''
    if len(found_quantitative_covariates) > 0:
        quant_covars_join = ','.join(found_quantitative_covariates)
        suffix = suffix + '--covarColList PC{1:10},age,age_squared,sex,' + quant_covars_join + ' '
    else:
        suffix = suffix + '--covarColList PC{1:10},age,age_squared,sex '

    if len(found_categorical_covariates) > 0:
        cat_covars_join = ','.join(found_categorical_covariates)
        suffix = suffix + '--catCovarList wes_batch,' + cat_covars_join + ' '
    else:
        suffix = suffix + '--catCovarList wes_batch '

    if is_binary:
        suffix = suffix + '--bt --firth --approx'

    return suffix


# This method is a slightly incorrect as it converts a string genotype into a 0,2 range, but as a float instead of
# as the expected int. This is because:
# 1) integers cannot by NULL / NA in python
# 2) I use pandas to handle most of my genotype information, which can only use floats as a nullable datatype
# Thus I convert to a float and then cast to an integer in string format if required by the output that uses this
# method
def gt_to_float(gt: str) -> float:
    if gt == '0/0':
        return 0
    elif gt == '0/1':
        return 1
    elif gt == '1/1':
        return 2
    else:
        return float('NaN')


# Downloads a dxfile and uses it's actual name as given by dxfile.describe()
def download_dxfile_by_name(file: Union[dict, str], print_status: bool = True) -> str:

    curr_dxfile = dxpy.DXFile(file)
    curr_filename = curr_dxfile.describe()['name']

    if print_status:
        print(f'Downloading file {curr_filename} ({curr_dxfile.get_id()})')
    dxpy.download_dxfile(curr_dxfile.get_id(), curr_filename)

    return curr_filename


# This function will locate an associated tbi/csi index:
def find_index(parent_file: dxpy.DXFile, index_suffix: str) -> dxpy.DXFile:

    # Describe the file to get attributes:
    file_description = parent_file.describe(fields={'folder': True, 'name': True, 'project': True})

    # First set the likely details of the corresponding index:
    project_id = file_description['project']
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