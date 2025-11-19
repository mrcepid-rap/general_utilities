import csv
from dataclasses import dataclass

import pytest

from pathlib import Path
from general_utilities.import_utils.module_loader.association_pack import ProgramArgs
from general_utilities.import_utils.module_loader.ingest_data import IngestData
from import_utils.file_handlers.input_file_handler import InputFileHandler

test_data_dir = Path(__file__).parent / "test_data"
linear_model_test_data_dir = test_data_dir / "linear_model/"

@dataclass
class TestProgramArgs(ProgramArgs):

    def __post_init__(self):
        self._check_opts()

    def _check_opts(self):
        pass

@pytest.fixture()
def program_arguments(request):
    arguments = TestProgramArgs(
        phenofile = [InputFileHandler(linear_model_test_data_dir / 'phenotype.tsv')],
        phenoname = 'phenotype',
        covarfile = InputFileHandler(linear_model_test_data_dir / 'other_covariatesv2.covariates'),
        categorical_covariates = request.param['categorical_covariates'],
        quantitative_covariates = request.param['quantitative_covariates'],
        is_binary = False,
        sex = request.param['sex'],
        exclusion_list = None,
        inclusion_list = None,
        transcript_index = InputFileHandler(linear_model_test_data_dir / 'transcripts.tsv.gz'),
        base_covariates = InputFileHandler(linear_model_test_data_dir / 'base_covariates.covariates'),
        ignore_base = request.param['ignore_base'],
    )
    return arguments

base_categorical_covariates = {'batch', 'array_batch'}
base_quantitative_covariates = set([f'PC{PC}' for PC in range(1, 41)] + \
                                   ['age', 'age_squared', 'sex'])
base_covariates = base_categorical_covariates.union(base_quantitative_covariates)

@pytest.mark.parametrize("program_arguments, expected_fields, expected_indv",
                         [
                             ({'categorical_covariates': ['batman'], 'quantitative_covariates': [], 'sex': 2, 'ignore_base': False},
                              base_covariates.union({'FID','IID','phenotype','batman'}),
                              10000),
                             ({'categorical_covariates': ['batman'], 'quantitative_covariates': [], 'sex': 2, 'ignore_base': True},
                              {'FID','IID','phenotype','batman'},
                              10000),
                             ({'categorical_covariates': ['batman'], 'quantitative_covariates': [], 'sex': 1, 'ignore_base': False},
                              base_covariates.union({'FID','IID','phenotype','batman'}),
                              4472),
                             ({'categorical_covariates': ['batman'], 'quantitative_covariates': [], 'sex': 0, 'ignore_base': False},
                              base_covariates.union({'FID','IID','phenotype','batman'}),
                              5528),
                             ({'categorical_covariates': ['batman'], 'quantitative_covariates': ['robin'], 'sex': 2, 'ignore_base': False},
                              base_covariates.union({'FID','IID','phenotype','batman','robin'}),
                              8996),
                             ({'categorical_covariates': ['batman'], 'quantitative_covariates': ['robin'], 'sex': 1, 'ignore_base': True},
                              {'FID','IID','phenotype','batman','robin'},
                              4018)
                         ],
                         indirect=['program_arguments'])
def test_module_loader(program_arguments, expected_fields, expected_indv):

    ingested_data = IngestData(program_arguments)
    association_pack = ingested_data.get_association_pack()

    # Adjust if we are not using both sexes
    if program_arguments.sex != 2 and program_arguments.ignore_base is False:
        expected_fields.remove('sex')

    with association_pack.final_covariates.open('r') as covariate_check:
        covariate_csv = csv.DictReader(covariate_check, delimiter=' ')
        total_lines = 0

        fields = set(covariate_csv.fieldnames)
        assert expected_fields == fields
        assert set(association_pack.found_categorical_covariates + association_pack.found_quantitative_covariates) == set(expected_fields - {'FID','IID','phenotype'})
        assert association_pack.pheno_names == ['phenotype']

        for _ in covariate_csv:
            total_lines+=1

        assert total_lines == expected_indv

    with association_pack.inclusion_samples.open('r') as inclusion_check:
        total_lines = sum(1 for _ in inclusion_check)
        assert total_lines == expected_indv

    with association_pack.exclusion_samples.open('r') as exclusion_check:
        total_lines = sum(1 for _ in exclusion_check)
        assert total_lines == 10000 - expected_indv

