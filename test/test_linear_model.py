import csv
import math
import pytest
import pandas as pd
import statsmodels.api as sm

from pathlib import Path

from general_utilities.association_resources import build_transcript_table
from general_utilities.linear_model.linear_model import linear_model_null, run_linear_model
from general_utilities.import_utils.import_lib import TarballType
from general_utilities.linear_model.linear_model import load_linear_model_genetic_data
from general_utilities.linear_model.proccess_model_output import process_model_outputs

test_data_dir = Path(__file__).parent / "test_data"
linear_model_test_data_dir = test_data_dir / "linear_model/"

@pytest.fixture
def phenofile(tmp_path) -> Path:
    phenofile = linear_model_test_data_dir / 'phenotype.tsv'
    base_covars = linear_model_test_data_dir / 'base_covariates.covariates'
    add_covars = linear_model_test_data_dir / 'other_covariates.covariates'

    test_pheno_covars = tmp_path / 'test_phenotype.tsv'

    with phenofile.open('r') as pheno_reader,\
          base_covars.open('r') as base_reader,\
          add_covars.open('r') as add_reader,\
          test_pheno_covars.open('w') as test_writer:

        pheno_csv = csv.DictReader(pheno_reader, delimiter='\t')
        base_csv = csv.DictReader(base_reader, delimiter='\t')
        add_csv = csv.DictReader(add_reader, delimiter='\t')
        fieldnames = pheno_csv.fieldnames + base_csv.fieldnames[2:] + add_csv.fieldnames[2:] + ['age_squared']

        test_pheno_csv = csv.DictWriter(test_writer, fieldnames=fieldnames, delimiter=' ')

        test_pheno_csv.writeheader()

        data_dict = {}
        for row in pheno_csv:
            data_dict[row['FID']] = row

        for row in base_csv:
            id = row['FID']
            row.pop('FID')
            row.pop('IID')
            row['age_squared'] = int(row['age']) ** 2
            data_dict[id].update(row)

        for row in add_csv:
            id = row['FID']
            row.pop('FID')
            row.pop('IID')
            data_dict[id].update(row)

        for sample, row in data_dict.items():
            test_pheno_csv.writerow(row)

    return test_pheno_covars

@pytest.fixture
def unpacked_tarball(tmp_path, request) -> Path:
    """Unpacks the tarball to a temporary directory."""

    import tarfile
    tarball = tarfile.open(linear_model_test_data_dir / f'{request.param}.tar.gz', 'r:gz')
    tarball.extractall(path=tmp_path)
    tarball.close()
    return tmp_path / request.param

@pytest.fixture
def transcripts_table() -> pd.DataFrame:
    """Returns a DataFrame with ENST and gene names."""
    transcripts_path = test_data_dir / 'transcripts.tsv.gz'
    return build_transcript_table(transcripts_path, False)

def test_linear_model_null(phenofile):
    """Test the creation of the null model for a quantitative phenotype.

    Note: Binary traits are not yet tested by this pipeline as we do not yet have a full implementation of test data.
    """

    null_model = linear_model_null(phenofile,
                                   phenotype='phenotype',
                                   is_binary=False,
                                   ignore_base=False,
                                   found_quantitative_covariates=[],
                                   found_categorical_covariates=['batman']
                                   )

    assert null_model.pheno_name == 'phenotype'
    assert null_model.phenotypes.index.name == 'FID'
    assert null_model.phenotypes.shape == (10000, 16) # has_var isn't in the table yet, it is added later

    assert isinstance(null_model.model_family, sm.families.Gaussian)
    assert null_model.model_formula == 'phenotype ~ PC1 + PC2 + PC3 + PC4 + PC5 + PC6 + PC7 + PC8 + PC9 + PC10 + age + age_squared + sex + C(wes_batch) + C(batman) + has_var'

    assert null_model.null_model.index.name == 'FID'
    assert null_model.null_model.shape == (10000, 1)


@pytest.mark.parametrize("unpacked_tarball, expected_genes_path, bgen_prefix, tarball_type",
                         zip(
                             ("HC_PTV-MAF_001", "HC_PTV-MAF_001", "HC_PTV-MAF_001_GENE", "HC_PTV-MAF_001_SNP"),
                             (linear_model_test_data_dir / 'expt_genes.PTV.tsv', linear_model_test_data_dir / 'expt_genes.PTV_chunk1.tsv', linear_model_test_data_dir / 'expt_genes.GENE.tsv', linear_model_test_data_dir / 'expt_genes.SNP.tsv'),
                             (None, 'chr1_chunk1', None, None),
                             (TarballType.GENOMEWIDE, TarballType.GENOMEWIDE, TarballType.GENE, TarballType.SNP)
                         ), indirect=["unpacked_tarball"])
def test_load_tarball_linear_model(unpacked_tarball, expected_genes_path, bgen_prefix, tarball_type):
    """Test loading genetic data from different tarball types."""

    tarball_path, genetic_data = load_linear_model_genetic_data(str(unpacked_tarball), tarball_type, bgen_prefix=bgen_prefix)

    expected_genes = pd.read_csv(expected_genes_path, header = None)

    assert set(genetic_data.index.names) == {'FID', 'ENST'}
    assert len(genetic_data.index.get_level_values('FID').unique()) == 10000
    assert set(genetic_data.index.get_level_values('ENST').unique()) == set(expected_genes[0])

@pytest.mark.parametrize("unpacked_tarball, expected_genes_path, tarball_type, is_binary",
                         zip(
                             ("HC_PTV-MAF_001", "HC_PTV-MAF_001", "HC_PTV-MAF_001_GENE", "HC_PTV-MAF_001_SNP"),
                             (linear_model_test_data_dir / 'expt_genes.PTV.tsv', linear_model_test_data_dir / 'expt_genes.PTV_chunk1.tsv', linear_model_test_data_dir / 'expt_genes.GENE.tsv', linear_model_test_data_dir / 'expt_genes.SNP.tsv'),
                             (TarballType.GENOMEWIDE, TarballType.GENOMEWIDE, TarballType.GENE, TarballType.SNP),
                             (False, False, False, False)
                         ), indirect=["unpacked_tarball"])
def test_run_linear_model(tmp_path, phenofile, transcripts_table, unpacked_tarball, expected_genes_path, tarball_type, is_binary):
    """Test running the linear model on different tarball types.

    This test re-runs the generation of the null model and loading of genetic data, but does not test the outputs as
    this has already been done above. The resulting data is then run through the linear model and the outputs checked.
    """

    null_model = linear_model_null(phenofile,
                                   phenotype='phenotype',
                                   is_binary=False,
                                   ignore_base=False,
                                   found_quantitative_covariates=[],
                                   found_categorical_covariates=['batman']
                                   )

    tarball_name, genetic_data = load_linear_model_genetic_data(unpacked_tarball, tarball_type, bgen_prefix=None)

    expected_genes = set()
    with expected_genes_path.open('r') as expected_genes_reader:
        for gene in expected_genes_reader:
            expected_genes.add(gene.rstrip())

    final_models = []

    for gene in genetic_data.index.get_level_values('ENST').unique():
        print(tarball_name)
        result = run_linear_model(null_model, genetic_data, gene, tarball_name, is_binary=False, always_run_corrected=False)
        final_models.append(result)

        assert result.pheno_name == 'phenotype'
        assert result.ENST == gene
        assert result.mask_name == tarball_name
        assert result.n_model == 10000
        assert result.n_noncar_unaffected == 0  #TODO: Implement binary models!

        #TODO: When we implement full data, check actual p. values
        if result.n_car <= 2:
            assert math.isnan(result.p_val_init)
            assert result.model_run == False
        else:
            assert not math.isnan(result.p_val_init)
            assert result.model_run == True

        if result.p_val_init < 1e-4:
            print(result)
            assert not math.isnan(result.p_val_full)
        else:
            assert math.isnan(result.p_val_full)

    found_genes = {result.ENST for result in final_models}
    assert len(found_genes.intersection(expected_genes)) == len(expected_genes)

    expected_fields = ['ENST', 'MASK', 'MAF', 'pheno_name', 'n_car', 'cMAC', 'n_model', 'model_run', 'p_val_init',
                       'p_val_full', 'effect', 'std_err', 'n_noncar_affected', 'n_noncar_unaffected', 'n_car_affected',
                       'n_car_unaffected']
    if tarball_type == TarballType.GENOMEWIDE:
        expected_fields[1:1] = ['chrom', 'start', 'end', 'ENSG', 'MANE', 'transcript_length', 'SYMBOL',
                                'CANONICAL', 'BIOTYPE', 'cds_length', 'coord', 'manh.pos']

    output_files = process_model_outputs(final_models, tmp_path / f'{tarball_name}.genes.glm.stats.tsv', tarball_type, transcripts_table)

    if tarball_type == TarballType.GENOMEWIDE:
        assert output_files == [tmp_path / f'{tarball_name}.genes.glm.stats.tsv.gz',
                                tmp_path / f'{tarball_name}.genes.glm.stats.tsv.gz.tbi']
    else:
        assert output_files == [tmp_path / f'{tarball_name}.genes.glm.stats.tsv.gz']

    test_output = pd.read_csv(output_files[0], sep='\t')
    assert test_output.columns.tolist() == expected_fields
    assert test_output['ENST'].isin(expected_genes).value_counts()[True] == len(expected_genes)
