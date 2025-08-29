import re
from pathlib import Path
from typing import List, Dict

import pandas as pd
from importlib_resources import files
from scipy.io import mmwrite

from general_utilities.association_resources import replace_multi_suffix
from general_utilities.bgen_utilities.genotype_matrix import generate_csr_matrix_from_bgen
from general_utilities.bgen_utilities.genotype_matrix import make_variant_list, GeneInformation
from general_utilities.job_management.command_executor import DockerMount, build_default_command_executor
from general_utilities.job_management.command_executor import CommandExecutor
from import_utils.import_lib import BGENInformation


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
    :param cmd_executor: The command executor to use. Defaults to the standard burdentesting Docker file.
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


def build_staar_variant_list(variant_file: Path, bgen_path, index_path, sample_path, gene) -> Dict[str, GeneInformation]:




def staar_genes(staar_null_path: Path, tarball_prefix: str, pheno_name: str, bgen_info: Dict[str, BGENInformation],
                bgen_prefix: str = None, gene_info_path: Path = None,
                cmd_executor: CommandExecutor = build_default_command_executor()) -> tuple:
    """Run rare variant association testing using STAAR.



    """

    tarball_path = Path(tarball_prefix)

    staar_variants_list = tarball_path.parent.glob(replace_multi_suffix(tarball_path, '.*.STAAR.variants_table.tsv').name)
    # If requested to load a single bgen prefix, filter the list to only include that prefix
    if bgen_prefix is not None:
        if tarball_path.parent / f'{tarball_path.name}.{bgen_prefix}.STAAR.variants_table.tsv' in staar_variants_list:
            staar_variants_list = [tarball_path.parent / f'{tarball_path.name}.{bgen_prefix}.STAAR.variants_table.tsv']
        else:
            raise ValueError(
                f'BGEN prefix {bgen_prefix} not found in tarball {tarball_path.name}.tar.gz. Please check the input.')

    for staar_variants in staar_variants_list:

        variant_table = pd.read_csv(staar_variants, delimiter='\t')
        variant_matrix = make_variant_list(variant_table)

        # get the bgen prefix out of the staar variants path with a regex:
        current_prefix = re.search(rf'{tarball_path.name}\.({bgen_prefix})\.STAAR\.variants_table\.tsv', staar_variants.name).group(1)
        bgen_path = bgen_info[current_prefix]['bgen'].get_file_handle()
        sample_path = bgen_info[current_prefix]['sample'].get_file_handle()
        index_path = bgen_info[current_prefix]['index'].get_file_handle()  # have to do this otherwise the file isn't in the right place

        # Make sure we have the samples
        staar_samples = tarball_path.parent / f'{tarball_path.name}.{current_prefix}.STAAR.samples_table.tsv'

        for gene, gene_info in variant_matrix.items():


            gene_matrix, _ = generate_csr_matrix_from_bgen(bgen_path=bgen_path, sample_path=sample_path, variant_filter_list=gene_info['vars'],
                                                           chromosome=gene_info['chrom'], start=gene_info['min'], end=gene_info['max'],
                                                           should_collapse_matrix=False)

            staar_matrix = Path(f'{tarball_path}.{current_prefix}.STAAR.matrix.rds')
            mmwrite(staar_matrix, gene_matrix)

            # I have made a custom script in order to generate STAAR per-gene models that is installed using pip
            # as part of the general_utilities package. We can extract the system location of this script:
            r_script = files('general_utilities.linear_model.R_resources').joinpath('runSTAAR_Genes.R')

            script_mount = DockerMount(r_script.parent,
                                       Path('/scripts/'))

            output_path = Path(f'{gene}.{pheno_name}.STAAR_results.tsv')

            # This generates a text output file of p.values
            # See the README.md for more information on these parameters
            cmd = f'Rscript /scripts/{r_script.name} ' \
                  f'/test/{staar_matrix.name} ' \
                  f'/test/{staar_variants.name} ' \
                  f'/test/{staar_samples.name} ' \
                  f'/test/{staar_null_path.name} ' \
                  f'/test/{output_path.name}'

            cmd_executor.run_cmd_on_docker(cmd, docker_mounts=[script_mount])

    return tarball_prefix, chromosome, phenoname