from pathlib import Path
from typing import List
from importlib_resources import files

from general_utilities.job_management.command_executor import DockerMount, build_default_command_executor
from job_management.command_executor import CommandExecutor


# Generate the NULL model for STAAR
def staar_null(phenofile: Path, phenotype: str, is_binary: bool, ignore_base: bool,
               found_quantitative_covariates: List[str], found_categorical_covariates: List[str],
               sex: int, sparse_kinship_file: Path, sparse_kinship_samples: Path,
               cmd_executor: CommandExecutor = build_default_command_executor()) -> Path:
    """This method wraps an R script that generates the STAAR Null model.

    The script is included as a package resource in the general_utilities.linear_model.R_resources package. The file
    is included in the wheel on installation by pip and can be accessed using importlib_resources. Thus, the script
    location is hardcoded into this method.

    :param phenofile: The path to the phenotype file. This file must be space or tab delimited and contain a header row.
    :param phenotype: The name of the phenotype column in the phenotype file.
    :param is_binary: Whether the phenotype is binary (True) or quantitative (False).
    :param ignore_base: Whether to ignore the base covariates (True) or include them (False).
    :param found_quantitative_covariates: A list of additional quantitative covariates to include in the model.
    :param found_categorical_covariates: A list of additional categorical covariates to include in the model.
    :param sex: Sex of samples that will be analysed. Required for STAAR as the model used does not accept covariates
        with a single value (e.g., if just running males / females).
    :param sparse_kinship_file: The path to the sparse kinship matrix file in Matrix Market format.
    :param sparse_kinship_samples: The path to the sample IDs file for the sparse kinship matrix.
    """

    r_script = files('general_utilities.linear_model.R_resources').joinpath('runSTAAR_Null.R')

    script_mount = DockerMount(r_script.parent,
                               Path('/scripts/'))

    # This script then generates an RDS output file containing the NULL model
    # See the README.md for more information on these parameters
    cmd = f'Rscript /scripts/{r_script.name} ' \
          f'/test/{phenofile.name} ' \
          f'{phenotype} ' \
          f'{is_binary} ' \
          f'/test/{sparse_kinship_file.name} ' \
          f'/test/{sparse_kinship_samples.name} '

    # Set covariates for the model
    if ignore_base:
        quant_covars = []
        cat_covars = []
    else:
        quant_covars = [f'PC{PC}' for PC in range(1, 11)] + ['age', 'age_squared']
        if sex == 2:
            quant_covars.append('sex')
        cat_covars = ['wes_batch']

    quant_covars.extend(found_quantitative_covariates)
    cat_covars.extend(found_categorical_covariates)

    if len(quant_covars) > 0:
        cmd += f'{",".join(quant_covars)} '
    else:
        cmd += f'NULL '
    if len(cat_covars) > 0:
        cmd += f'{",".join(cat_covars)} '
    else:
        cmd += f'NULL '

    cmd_executor.run_cmd_on_docker(cmd, docker_mounts=[script_mount])

    return Path(f'{phenotype}.STAAR_null.rds')

# Run rare variant association testing using STAAR
# Returns the finished chromosome to aid in output file creation
def staar_genes(tarball_prefix: str, chromosome: str, phenoname: str, has_gene_info: bool) -> tuple:

    # I have made a custom script in order to generate STAAR per-gene models that is installed using pip
    # as part of the general_utilities package. We can extract the system location of this script:
    r_script = files('general_utilities.linear_model.R_resources').joinpath('runSTAAR_Genes.R')

    script_mount = DockerMount(r_script.parent,
                               Path('/scripts/'))
    cmd_executor = build_default_command_executor()

    # This generates a text output file of p.values
    # See the README.md for more information on these parameters
    cmd = f'Rscript /scripts/{r_script.name} ' \
          f'/test/{tarball_prefix}.{chromosome}.STAAR.matrix.rds ' \
          f'/test/{tarball_prefix}.{chromosome}.variants_table.STAAR.tsv ' \
          f'/test/{phenoname}.STAAR_null.rds ' + \
          f'{phenoname} ' \
          f'{tarball_prefix} ' \
          f'{chromosome} '

    # If a subset of genes has been requested, do it here.
    if has_gene_info:
        cmd += f'/test/staar.gene_list'
    else:
        cmd += f'none'  # This is always none when doing a genome-wide study.

    cmd_executor.run_cmd_on_docker(cmd, docker_mounts=[script_mount])

    return tarball_prefix, chromosome, phenoname