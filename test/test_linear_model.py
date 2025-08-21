import csv
import math
from dataclasses import asdict

import pandas as pd
import pytest
import statsmodels.api as sm

from pathlib import Path

from general_utilities.linear_model.linear_model import linear_model_null, run_linear_model
from import_utils.import_lib import TarballType
from linear_model.linear_model import load_tarball_linear_model

test_data_dir = Path(__file__).parent / "test_data/linear_model/"

@pytest.fixture
def phenofile(tmp_path) -> Path:
    phenofile = test_data_dir / 'phenotype.tsv'
    base_covars = test_data_dir / 'base_covariates.covariates'
    add_covars = test_data_dir / 'other_covariates.covariates'

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
    tarball = tarfile.open(test_data_dir / f'{request.param}.tar.gz', 'r:gz')
    tarball.extractall(path=tmp_path)
    tarball.close()
    return tmp_path / request.param


def test_linear_model_null(phenofile):

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
                             (test_data_dir / 'expt_genes.PTV.tsv', test_data_dir / 'expt_genes.PTV_chunk1.tsv', test_data_dir / 'expt_genes.GENE.tsv', test_data_dir / 'expt_genes.SNP.tsv'),
                             (None, 'chr1_chunk1', None, None),
                             (TarballType.GENOMEWIDE, TarballType.GENOMEWIDE, TarballType.GENE, TarballType.SNP)
                         ), indirect=["unpacked_tarball"])
def test_load_tarball_linear_model(unpacked_tarball, expected_genes_path, bgen_prefix, tarball_type):

    tarball_path, genetic_data = load_tarball_linear_model(str(unpacked_tarball), tarball_type, bgen_prefix=bgen_prefix)

    expected_genes = pd.read_csv(expected_genes_path, header = None)

    assert set(genetic_data.index.names) == {'FID', 'ENST'}
    assert len(genetic_data.index.get_level_values('FID').unique()) == 10000
    assert set(genetic_data.index.get_level_values('ENST').unique()) == set(expected_genes[0])

@pytest.mark.parametrize("unpacked_tarball, expected_genes_path, tarball_type",
                         zip(
                             ("HC_PTV-MAF_001", "HC_PTV-MAF_001", "HC_PTV-MAF_001_GENE", "HC_PTV-MAF_001_SNP"),
                             (test_data_dir / 'expt_genes.PTV.tsv', test_data_dir / 'expt_genes.PTV_chunk1.tsv', test_data_dir / 'expt_genes.GENE.tsv', test_data_dir / 'expt_genes.SNP.tsv'),
                             (TarballType.GENOMEWIDE, TarballType.GENOMEWIDE, TarballType.GENE, TarballType.SNP)
                         ), indirect=["unpacked_tarball"])
def test_run_linear_model(phenofile, unpacked_tarball, expected_genes_path, tarball_type):

    null_model = linear_model_null(phenofile,
                                   phenotype='phenotype',
                                   is_binary=False,
                                   ignore_base=False,
                                   found_quantitative_covariates=[],
                                   found_categorical_covariates=['batman']
                                   )

    tarball_name, genetic_data = load_tarball_linear_model(str(unpacked_tarball), tarball_type, bgen_prefix=None)

    expected_genes = set()
    with expected_genes_path.open('r') as expected_genes_reader:
        for gene in expected_genes_reader:
            expected_genes.add(gene.rstrip())

    found_genes = set()

    for gene in genetic_data.index.get_level_values('ENST').unique():
        result = run_linear_model(null_model, genetic_data, gene, tarball_name, is_binary=False, always_run_corrected=False)

        assert result.pheno_name == 'phenotype'
        assert result.ENST == gene
        assert result.maskname == tarball_name
        assert result.n_model == 10000
        assert result.n_noncar_unaffected == 0  #TODO: Implement binary models!

        found_genes.add(result.ENST)

        #TODO: When we implement full data, check actual p. values
        if result.n_car <= 2:
            assert math.isnan(result.p_val_init)
        else:
            assert not math.isnan(result.p_val_init)

        if result.p_val_init < 1e-4:
            print(result)
            assert not math.isnan(result.p_val_full)
        else:
            assert math.isnan(result.p_val_full)

    assert len(found_genes.intersection(expected_genes)) == len(expected_genes)