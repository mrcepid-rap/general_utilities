from typing import List
from importlib_resources import files

from general_utilities.association_resources import run_cmd


# Generate the NULL model for STAAR
def staar_null(phenoname: str, is_binary: bool,
               found_quantitative_covariates: List[str], found_categorical_covariates: List[str]) -> None:

    # I have made a custom script in order to generate the STAAR Null model that is installed using pip
    # as part of the general_utilities package. We can extract the system location of this script:
    r_script = files('general_utilities.linear_model.R_resources').joinpath('runSTAAR_Null.R')

    # This script then generates an RDS output file containing the NULL model
    # See the README.md for more information on these parameters
    cmd = f'Rscript /scripts/{r_script.name} ' \
          f'/test/phenotypes_covariates.formatted.txt ' \
          f'{phenoname} ' \
          f'{is_binary} '
    if len(found_quantitative_covariates) > 0:
        cmd += f'{",".join(found_quantitative_covariates)} '
    else:
        cmd += f'NULL '
    if len(found_categorical_covariates) > 0:
        cmd += f'{",".join(found_categorical_covariates)} '
    else:
        cmd += f'NULL '
    run_cmd(cmd, is_docker=True, docker_image='egardner413/mrcepid-burdentesting',
            docker_mounts=[f'{r_script.parent}/:/scripts/'])


# Run rare variant association testing using STAAR
# Returns the finished chromosome to aid in output file creation
def staar_genes(tarball_prefix: str, chromosome: str, phenoname: str, has_gene_info: bool) -> tuple:

    # I have made a custom script in order to generate STAAR per-gene models that is installed using pip
    # as part of the general_utilities package. We can extract the system location of this script:
    r_script = files('general_utilities.linear_model.R_resources').joinpath('runSTAAR_Genes.R')

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

    run_cmd(cmd, is_docker=True, docker_image='egardner413/mrcepid-burdentesting',
            docker_mounts=[f'{r_script.parent}/:/scripts/'])

    return tarball_prefix, chromosome, phenoname
