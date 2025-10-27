import bgen
import dxpy
import numpy as np
import pandas as pd
import statsmodels.api as sm

from pathlib import Path
from typing import Tuple, List
from scipy.io import mmread
from dataclasses import dataclass, field

from general_utilities.mrc_logger import MRCLogger
from general_utilities.import_utils.import_lib import TarballType
from general_utilities.association_resources import replace_multi_suffix

LOGGER = MRCLogger(__name__).get_logger()

@dataclass
class LinearModelPack:
    """
    This is a helper class to store information for running linear models

    :attr phenotypes: Phenotypes/covariates for every individual in a Pandas df
    :attr pheno_name: name of the phenotype of interest
    :attr model_family: family from statsmodels families
    :attr model_formula: Formatted formula for all linear models
    :attr null_model: a null model containing just IID and residuals
    :attr n_model: Number of individuals with values in the pheno/covariate file (do this here to save compute)
    """

    phenotypes: pd.DataFrame
    pheno_name: str
    model_family: sm.families
    model_formula: str
    null_model: pd.DataFrame
    n_model: int = field(init=False)

    def __post_init__(self):
        self.n_model = len(self.phenotypes)


@dataclass
class LinearModelResult:
    """
    Class that holds results from linear models

    :attr ENST: ENST ID of the gene tested
    :attr mask_name: Name of the mask used for this gene (e.g., PTV)
    :attr pheno_name: Name of the phenotype tested
    :attr n_car: Number of carriers of the variant
    :attr cMAC: Carrier minor allele count
    :attr n_model: Number of individuals in the model
    :attr model_run: Boolean indicating if the model was run successfully
    :attr p_val_init: Initial p-value from the null model
    :attr p_val_full: Full model p-value (if p_val_init < nominal significance of 1e-4)
    :attr effect: Effect size of the variant on the phenotype
    :attr std_err: Standard error of the effect size
    :attr n_noncar_affected: Number of non-carriers affected by the phenotype
    :attr n_noncar_unaffected: Number of non-carriers unaffected by the phenotype
    :attr n_car_affected: Number of carriers affected by the phenotype
    :attr n_car_unaffected: Number of carriers unaffected by the phenotype
    """

    ENST: str
    mask_name: str
    pheno_name: str
    n_car: int
    cMAC: int
    n_model: int
    model_run: bool
    p_val_init: float = float('nan')
    p_val_full: float = float('nan')
    effect: float = float('nan')
    std_err: float = float('nan')
    n_noncar_affected: int = 0
    n_noncar_unaffected: int = 0
    n_car_affected: int = 0
    n_car_unaffected: int = 0

    def set_carrier_stats(self, n_noncar_affected: int, n_noncar_unaffected: int,
                          n_car_affected: int, n_car_unaffected: int) -> None:
        """Setter function to set carrier stats for the linear model result all in one function call

        :param n_noncar_affected: Number of non-carriers affected by the phenotype
        :param n_noncar_unaffected: Number of non-carriers unaffected by the phenotype
        :param n_car_affected: Number of carriers affected by the phenotype
        :param n_car_unaffected: Number of carriers unaffected by the phenotype
        :return: None
        """

        self.n_noncar_affected = n_noncar_affected
        self.n_noncar_unaffected = n_noncar_unaffected
        self.n_car_affected = n_car_affected
        self.n_car_unaffected = n_car_unaffected


def linear_model_null(phenofile: Path, phenotype: str, is_binary: bool, ignore_base: bool,
                      found_quantitative_covariates: List[str],
                      found_categorical_covariates: List[str]) -> LinearModelPack:
    """Perform initial linear model setup.

    This function loads the phenotype and covariate data, checks if the phenotype is binary, and sets up the
    appropriate model family and formula. It also builds a null model to extract residuals for further analysis.

    :param phenofile: Path to the phenotype and covariate file.
    :param phenotype: Name of the phenotype to analyze.
    :param is_binary: Boolean indicating if the phenotype is binary.
    :param ignore_base: Boolean indicating if base covariates should be ignored.
    :param found_quantitative_covariates: List of additional quantitative covariates to include in the model.
    :param found_categorical_covariates: List of additional categorical covariates to include in the model.
    :return: A LinearModelPack object containing the model setup.
    """

    # load covariates and phenotypes
    pheno_covars = pd.read_csv(phenofile,
                               sep=" ",
                               index_col="FID",
                               dtype={'IID': str})
    pheno_covars.index = pheno_covars.index.astype(str)

    # Check if a binary trait and make sure there is more than one level after filtering
    if (is_binary is True and len(pheno_covars[phenotype].value_counts()) > 1) or \
            is_binary is False:

        # Decide what model family to use:
        if is_binary:
            family = sm.families.Binomial()
        else:
            family = sm.families.Gaussian()

        # And finally define the formula to be used by all models:
        if ignore_base:
            quant_covars = []
            cat_covars = []
        else:
            quant_covars = [f'PC{PC}' for PC in range(1, 11)] + ['age', 'age_squared', 'sex']
            cat_covars = ['wes_batch']

        quant_covars.extend(found_quantitative_covariates)
        cat_covars.extend(found_categorical_covariates)

        columns = [phenotype] + quant_covars + cat_covars
        cat_covars = [f'C({covar})' for covar in cat_covars]

        if len(quant_covars) + len(cat_covars) == 0:
            form_null = f'{phenotype} ~ 1'
        else:
            covars = ' + '.join(quant_covars + cat_covars)
            form_null = f'{phenotype} ~ {covars}'

        form_full = f'{form_null} + has_var'

        # Just make sure we subset if doing phewas to save space...
        pheno_covars = pheno_covars[columns]

        # Build the null model and extract residuals:
        sm_results_null = sm.GLM.from_formula(form_null, data=pheno_covars, family=sm.families.Gaussian()).fit()
        null_table = sm_results_null.resid_response.to_frame()
        null_table = null_table.rename(columns={0: 'resid'})

        # Start building a set of models we want to test.
        return LinearModelPack(phenotypes=pheno_covars,
                               pheno_name=phenotype,
                               model_family=family,
                               model_formula=form_full,
                               null_model=null_table)
    else:
        raise dxpy.AppError(f'Phenotype {phenotype} has no individuals after filtering, exiting...')


def load_linear_model_genetic_data(tarball_prefix: str, tarball_type: TarballType, bgen_prefix: str = None) -> Tuple[str, pd.DataFrame]:
    """Load a tarball containing BGEN files for linear model association testing.

    This method decides which function to call (either :func:'load_mask_linear_model' or
    :func:'load_gene_or_snp_linear_model') based on the type of tarball provided. For each tarball prefix,
    we want to make ONE pandas DataFrame for efficient querying of variants. The format of the DataFrame is:

    {
    'ENST': [ENST IDs],
    'FID': [Sample IDs],
    'gts': [genotype values]
    }

    And is indexed by both ENST and FID.

    :param tarball_prefix: The prefix of the tarball to load.
    :param tarball_type: The type of the tarball (SNP, GENE, or GENOMEWIDE).
    :param bgen_prefix: Optional prefix to filter BGEN files by. If None, all BGEN files in the tarball are loaded.
    :return: A tuple containing the tarball name and a pandas DataFrame with genetic data indexed by ENST and FID.
    """

    LOGGER.info(f'Loading tarball prefix: {tarball_prefix}')

    # Convert to a path object to allow for file operations.
    tarball_path = Path(tarball_prefix)

    if tarball_type == TarballType.GENOMEWIDE:
        genetic_data = load_mask_genetic_data(tarball_path, bgen_prefix=bgen_prefix)
    elif tarball_type == TarballType.GENE:
        genetic_data = load_gene_or_snp_genetic_data(replace_multi_suffix(tarball_path, '.GENE.STAAR.mtx'), tarball_type.value)
    elif tarball_type == TarballType.SNP:
        genetic_data = load_gene_or_snp_genetic_data(replace_multi_suffix(tarball_path, '.SNP.STAAR.mtx'), tarball_type.value)
    else:
        raise ValueError(f'Unexpected tarball type {tarball_type} encountered for tarball prefix {tarball_prefix}')

    LOGGER.info(f'Finished loading tarball prefix: {tarball_prefix}')

    return tarball_path.name, genetic_data


def load_mask_genetic_data(tarball_path: Path, bgen_prefix: str = None) -> pd.DataFrame:
    """Load a genome-wide burden mask with multiple genes into the required format.

    Note that the prefix of the tarball is used to identify the BGEN files within the tarball. The tarball should
    contain BGEN files for all 'chunks' processed during a run of CollapseVariants.

    :param tarball_path: The path to the tarball containing BGEN files.
    :param bgen_prefix: Optional prefix to filter BGEN files by. If None, all BGEN files in the tarball are loaded.
    :return: A pandas DataFrame with genetic data indexed by ENST and FID.
    """

    geno_tables = []

    bolt_bgen_list = tarball_path.parent.glob(replace_multi_suffix(tarball_path, '.*.BOLT.bgen').name)
    # If requested to load a single bgen prefix, filter the list to only include that prefix
    if bgen_prefix is not None:
        if tarball_path.parent / f'{tarball_path.name}.{bgen_prefix}.BOLT.bgen' in bolt_bgen_list:
            bolt_bgen_list = [tarball_path.parent / f'{tarball_path.name}.{bgen_prefix}.BOLT.bgen']
        else:
            raise ValueError(
                f'BGEN prefix {bgen_prefix} not found in tarball {tarball_path.name}.tar.gz. Please check the input.')

    for bolt_bgen in bolt_bgen_list:

        with bgen.BgenReader(bolt_bgen, sample_path=replace_multi_suffix(bolt_bgen, '.sample'),
                             delay_parsing=False) as bgen_reader:

            for variant in bgen_reader:
                gene_name = variant.varid
                probability_array = variant.probabilities

                gt_filter = probability_array.max(axis=1) < 0.8
                gts = probability_array.argmax(axis=1)
                gts = np.where(gt_filter, np.nan, gts)

                geno_table = pd.DataFrame(data={'ENST': gene_name, 'FID': bgen_reader.samples, 'gt': gts})
                geno_tables.append(geno_table)

    # And concatenate the final data_frame together:
    # The structure is a multi-indexed pandas DataFrame, where:
    # Index 0 = ENST
    # Index 1 = FID
    genetic_data = pd.concat(geno_tables)
    genetic_data.set_index(keys=['ENST', 'FID'], inplace=True)

    return genetic_data


def load_gene_or_snp_genetic_data(matrix_file: Path, dummy_gene_name: str) -> pd.DataFrame:
    """Load a collapsed gene or SNP-based mask into the required format

    :param matrix_file: The path to the matrix file containing the collapsed gene or SNP data.
    :param dummy_gene_name: A dummy gene name to use for the ENST index in the DataFrame.
    :return: A pandas DataFrame with genetic data indexed by ENST and FID.
    """

    samples_table = matrix_file.with_suffix('.samples_table.tsv')

    # read and collapse the variant matrix
    variant_matrix = mmread(matrix_file)
    variant_matrix = variant_matrix.todense().sum(axis=1)
    variant_matrix = np.reshape(variant_matrix, (-1, len(variant_matrix)))
    variant_matrix = variant_matrix.tolist()[0]

    # Load the samples table
    samples = pd.read_csv(samples_table, sep='\t', dtype={'sampID': str})

    genetic_data = pd.DataFrame(data={'ENST': dummy_gene_name, 'FID': samples['sampID'], 'gt': variant_matrix})
    genetic_data.set_index(keys=['ENST', 'FID'], inplace=True)

    return genetic_data


def add_individuals_with_variant(model_frame: pd.DataFrame, indv_w_var: pd.DataFrame):
    """Helper function for run_linear_model() that makes a copy of a model and adds individuals w/ or w/o a variant as a
    binary 0/1 columns called 'has_var'.

    :param model_frame: A pandas DataFrame containing the model frame with residuals.
    :param indv_w_var: A pandas DataFrame containing individuals with variants, indexed by FID.
    :return: A pandas DataFrame with the 'has_var' column added, indicating whether each individual has the variant.
    """

    internal_frame = pd.DataFrame.copy(model_frame)

    # And then we merge in the genotype information
    internal_frame = pd.merge(internal_frame, indv_w_var, how='left', on='FID')
    internal_frame['has_var'] = internal_frame['gt'].transform(lambda x: 0 if np.isnan(x) else x)
    internal_frame = internal_frame.drop(columns=['gt'])

    return internal_frame


def run_linear_model(linear_model_pack: LinearModelPack, genotype_table: pd.DataFrame, gene: str,
                     mask_name: str, is_binary: bool, always_run_corrected: bool = False) -> LinearModelResult:
    """Run association testing using a GLM

    Successively iterate through each gene and run our model. This method extracts the pandas dataframe that is
    relevant to this particular gene, if it is actually in the index. Note that the typing error is due to pandas
    having two instances of DataFrame, one with multiple indices, and one with a single index. The former allows
    'levels' calls, the latter does not.

    :param linear_model_pack: A LinearModelPack object containing the model setup.
    :param genotype_table: A pandas DataFrame containing the genotype data indexed by ENST and FID.
    :param gene: The ENST ID of the gene to test.
    :param mask_name: The name of the mask used for this gene (e.g., PTV).
    :param is_binary: A boolean indicating if the phenotype is binary.
    :param always_run_corrected: A boolean indicating if the full model should always be run, even if the initial p-value is not significant.
    :return: A LinearModelResult object containing the results of the association test for this gene.
    """
    if gene in genotype_table.index.levels[0]:
        indv_w_var = genotype_table.loc[gene]

        # We have to make an internal copy as this pandas.DataFrame is NOT threadsafe
        internal_frame = add_individuals_with_variant(linear_model_pack.null_model, indv_w_var)
        n_car = len(internal_frame.loc[internal_frame['has_var'] >= 1])
        cMAC = internal_frame['has_var'].sum()

        if n_car <= 2: # Don't run models that won't converge
            gene_dict = LinearModelResult(gene, mask_name, linear_model_pack.pheno_name,
                                          n_car, cMAC, linear_model_pack.n_model, False)
        else:
            sm_results = sm.GLM.from_formula('resid ~ has_var',
                                             data=internal_frame,
                                             family=sm.families.Gaussian()).fit()

            internal_frame = add_individuals_with_variant(linear_model_pack.phenotypes, indv_w_var)

            # If we get a significant result here, re-test with the full model to get accurate
            # beta/p. value/std. err. OR if we are running a phewas, always calculate the full model
            if sm_results.pvalues['has_var'] < 1e-4 or always_run_corrected:

                sm_results_full = sm.GLM.from_formula(linear_model_pack.model_formula,
                                                      data=internal_frame,
                                                      family=linear_model_pack.model_family).fit()

                gene_dict = LinearModelResult(gene, mask_name, linear_model_pack.pheno_name,
                                              n_car, cMAC, sm_results.nobs, True,
                                              sm_results.pvalues['has_var'], sm_results_full.pvalues['has_var'],
                                              sm_results_full.params['has_var'], sm_results_full.bse['has_var'])

                # If we are dealing with a binary phenotype we also want to provide the "Fisher's" table
                if is_binary:
                    pheno_name = linear_model_pack.pheno_name
                    gene_dict.set_carrier_stats(
                        n_noncar_affected=len(internal_frame.query(f'has_var == 0 & {pheno_name} == 1')),
                        n_noncar_unaffected=len(internal_frame.query(f'has_var == 0 & {pheno_name} == 0')),
                        n_car_affected=len(internal_frame.query(f'has_var >= 1 & {pheno_name} == 1')),
                        n_car_unaffected=len(internal_frame.query(f'has_var >= 1 & {pheno_name} == 0')))

            else:
                gene_dict = LinearModelResult(gene, mask_name, linear_model_pack.pheno_name,
                                              n_car, cMAC, sm_results.nobs, True,
                                              p_val_init=sm_results.pvalues['has_var'])

                # If we are dealing with a binary phenotype we also want to provide the "Fisher's" table
                if is_binary:
                    pheno_name = linear_model_pack.pheno_name
                    gene_dict.set_carrier_stats(
                        n_noncar_affected=len(internal_frame.query(f'has_var == 0 & {pheno_name} == 1')),
                        n_noncar_unaffected=len(internal_frame.query(f'has_var == 0 & {pheno_name} == 0')),
                        n_car_affected=len(internal_frame.query(f'has_var >= 1 & {pheno_name} == 1')),
                        n_car_unaffected=len(internal_frame.query(f'has_var >= 1 & {pheno_name} == 0')))
    else:
        gene_dict = LinearModelResult(0, 0, linear_model_pack.n_model, gene, mask_name,
                                      linear_model_pack.pheno_name)

    return gene_dict
