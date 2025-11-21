import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict

import pandas as pd
from importlib_resources import files

from general_utilities.association_resources import replace_multi_suffix
from general_utilities.bgen_utilities.genotype_matrix import GeneInformation
from general_utilities.bgen_utilities.genotype_matrix import make_variant_list
from general_utilities.job_management.command_executor import CommandExecutor
from general_utilities.job_management.command_executor import build_default_command_executor


@dataclass
class STAARModelResult:
    """
    Class that holds results from STAAR models

    :attr ENST: ENST ID of the gene tested
    :attr mask_name: Name of the mask used for this gene (e.g., PTV)
    :attr pheno_name: Name of the phenotype tested
    :attr n_car: Number of carriers of the mask
    :attr cMAC: Carrier minor allele count
    :attr n_model: Number of individuals in the model
    :attr model_run: Boolean indicating if the model was run successfully
    :attr relatedness_correction: Boolean indicating if relatedness correction was applied during STAAR Null
    :attr p_val_O: P-value from the STAAR-Omnibus (O) test combining SKAT, burden, and ACAT
    :attr p_val_SKAT: P-value from the SKAT test
    :attr p_val_burden: P-value from the burden test
    :attr p_val_ACAT: P-value from the ACAT test
    """

    ENST: str
    mask_name: str
    pheno_name: str

    # These fields are automatically populated by the runSTAAR_genes.R script
    n_var: int
    cMAC: int
    n_model: int
    model_run: bool
    relatedness_correction: bool
    p_val_O: float = float('nan')
    p_val_SKAT: float = float('nan')
    p_val_burden: float = float('nan')
    p_val_ACAT: float = float('nan')

    def __post_init__(self):
        """Post-initialization to coerce float 'NaN' values from R to float nan in Python that Python reads as strings.
        """

        for p_str in ['p_val_O', 'p_val_SKAT', 'p_val_burden', 'p_val_ACAT']:
            p_val = getattr(self, p_str)
            if isinstance(p_val, str) and p_val == 'NaN':
                setattr(self, p_str, float('nan'))


# Generate the NULL model for STAAR
def staar_null(phenofile: Path, phenotype: str, is_binary: bool, ignore_base: bool,
               found_quantitative_covariates: List[str], found_categorical_covariates: List[str],
               sex: int, sparse_kinship_file: Path, sparse_kinship_samples: Path,
               cmd_executor: CommandExecutor = build_default_command_executor()) -> Path:
    """This method wraps an R script that generates the STAAR Null model.

    The STAAR model residualises the phenotype on covariates and a sparse kinship matrix to account for relatedness. The
    output is an RDS file of this null model that can be used in subsequent STAAR gene-based tests (:func:`staar_genes()`).

    The script is included as a package resource in the general_utilities.linear_model.R_resources package. The file
    is included in the wheel on installation by pip and can be accessed using importlib_resources; the script
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
    :param cmd_executor: The command executor to use. Defaults to the standard burdentesting Docker file. Primarily used
        for testing; users should not need to change this parameter.
    :return: The path to the output RDS file containing the STAAR null model.
    """

    r_script = files('general_utilities.linear_model.R_resources').joinpath('runSTAAR_Null.R')
    output_file = Path(f'{phenotype}.STAAR_null.rds')
    # This script then generates an RDS output file containing the NULL model
    # See the README.md for more information on these parameters
    cmd = f'Rscript {r_script} ' \
          f'{phenofile} ' \
          f'{phenotype} ' \
          f'{is_binary} ' \
          f'{sparse_kinship_file} ' \
          f'{sparse_kinship_samples} '

    # Set covariates for the model
    if ignore_base:
        quant_covars = []
        cat_covars = []
    else:
        quant_covars = [f'PC{PC}' for PC in range(1, 11)] + ['age', 'age_squared']
        if sex == 2:
            quant_covars.append('sex')
        cat_covars = ['batch']

    quant_covars.extend(found_quantitative_covariates)
    cat_covars.extend(found_categorical_covariates)

    if len(quant_covars) > 0:
        cmd += f'{",".join(quant_covars)} '
    else:
        cmd += 'NULL '
    if len(cat_covars) > 0:
        cmd += f'{",".join(cat_covars)} '
    else:
        cmd += 'NULL '

    cmd += f'{output_file}'

    cmd_executor.run_cmd_on_docker(cmd)

    return output_file


def load_staar_genetic_data(tarball_prefix: str, bgen_prefix: str = None) -> Dict[str, Dict[str, GeneInformation]]:
    """Load STAAR genetic data from a CollapseVariants output tarball.

    This method is a wrapper for :func:`make_variant_list()` in the bgen_utilities.genotype_matrix module. The method
    wraps this function in order to load all variant tables (except when provided with :param bgen_prefix:) in a
    CollapseVariants output tarball rather than load one variant table. It generates a dictionary of GeneInformation
    labelled by the BGEN prefix.

    :param tarball_prefix: The prefix of the tarball (i.e., the tarball file without the '.tar.gz' suffix).
    :param bgen_prefix: Optional specific BGEN prefix to load. If None, all prefixes in the tarball will be loaded.
    :return: A dictionary where keys are BGEN prefixes and values are dictionaries of GeneInformation objects keyed by
        ENST ID.
    """

    tarball_path = Path(tarball_prefix)

    staar_variants_list = tarball_path.parent.glob(
        replace_multi_suffix(tarball_path, '.*.STAAR.variants_table.tsv').name)
    # If requested to load a single bgen prefix, filter the list to only include that prefix
    if bgen_prefix is not None:
        if tarball_path.parent / f'{tarball_path.name}.{bgen_prefix}.STAAR.variants_table.tsv' in staar_variants_list:
            staar_variants_list = [tarball_path.parent / f'{tarball_path.name}.{bgen_prefix}.STAAR.variants_table.tsv']
        else:
            raise ValueError(
                f'BGEN prefix {bgen_prefix} not found in tarball {tarball_path.name}.tar.gz. Please check the input.')

    variant_matricies = {}

    for staar_variants in staar_variants_list:
        # extract BGEN prefix safely — match literal tarball prefix + prefix + STAAR
        pattern = rf'{re.escape(tarball_path.name)}\.(?P<prefix>[\w\-]+)\.STAAR\.variants_table\.tsv$'
        match = re.match(pattern, staar_variants.name)
        if not match:
            raise ValueError(f"Unable to extract BGEN prefix from filename: {staar_variants.name}")
        current_prefix = match.group("prefix")

        variant_table = pd.read_csv(staar_variants, delimiter='\t')
        variant_matrix = make_variant_list(variant_table)

        variant_matricies[current_prefix] = variant_matrix

    return variant_matricies


def staar_genes(staar_null_path: Path, pheno_name: str, gene: str, mask_name: str,
                staar_matrix: Path, staar_samples: Path, staar_variants: Path,
                out_dir: Path = Path(os.getcwd()),
                cmd_executor: CommandExecutor = build_default_command_executor()) -> STAARModelResult:
    """Run a single rare variant association test using STAAR.

    This method wraps an R script that runs a STAAR gene-based test for a single gene or pre-collapsed SNP / GENE mask.

    The :param staar_matrix: parameter can be generated in one of two ways. For genome-wide burden masks, :func:`generate_csr_matrix_from_bgen()`
    in the general_utilities.bgen_utilities.genotype_matrix module can be used to generate a sparse genotype matrix in Matrix Market format. For
    pre-collapsed SNP / GENE masks generated directly by the CollapseVariants tool, the sparse genotype matrix is included in the output tarball as
    '<mask_name>.[SNP/GENE].STAAR.mtx'.

    :param staar_null_path: The path to the STAAR null model RDS file generated by the :func:`staar_null()` method.
    :param pheno_name: The name of the phenotype column in the phenotype file.
    :param gene: The ENST ID of the gene to test.
    :param mask_name: The name of the mask to test (e.g., HC_PTV-MAF_01).
    :param staar_matrix: The path to the sparse genotype matrix in Matrix Market format.
    :param staar_samples: The path to the sample IDs file for the sparse genotype matrix (see CollapseVariants for more information).
    :param staar_variants: The path to the variants table file for the sparse genotype matrix (see CollapseVariants for more information).
    :param out_dir: The directory to write output files to. Defaults to the current working directory.
    :param cmd_executor: The command executor to use. Defaults to the standard burdentesting Docker file. Primarily used
        for testing; users should not need to change this parameter.
    :return: A STAARModelResult object containing the results of the association test.
    """

    # I have made a custom script in order to generate STAAR per-gene models that is installed using pip
    # as part of the general_utilities package. We can extract the system location of this script:
    r_script = files('general_utilities.linear_model.R_resources').joinpath('runSTAAR_Genes.R')

    output_path = out_dir / f'{gene}.{pheno_name}.STAAR_results.json'

    # This generates a text output file of p.values
    # See the README.md for more information on these parameters
    cmd = f'Rscript {r_script} ' \
          f'{staar_matrix} ' \
          f'{staar_variants} ' \
          f'{staar_samples} ' \
          f'{staar_null_path} ' \
          f'{gene} ' \
          f'{output_path}'

    cmd_executor.run_cmd_on_docker(cmd)

    # Read in the outputs and format into a model pack
    staar_json = json.load(output_path.open('r'))
    staar_json.update({'ENST': gene, 'mask_name': mask_name, 'pheno_name': pheno_name})
    staar_result = STAARModelResult(**staar_json)

    return staar_result
