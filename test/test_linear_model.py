import csv
import pytest
import statsmodels.api as sm

from pathlib import Path

from general_utilities.linear_model.linear_model import linear_model_null


test_data_source = Path(__file__).parent / "test_data/linear_model/"


@pytest.fixture
def phenofile(tmp_path) -> Path:
    phenofile = test_data_source / 'phenotype.tsv'
    base_covars = test_data_source / 'base_covariates.covariates'
    add_covars = test_data_source / 'other_covariates.covariates'

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


