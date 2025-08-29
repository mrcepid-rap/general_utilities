import csv
import shutil


import pandas as pd
import pytest

from pathlib import Path

from general_utilities.association_resources import build_transcript_table
from general_utilities.import_utils.file_handlers.input_file_handler import InputFileHandler
from general_utilities.import_utils.import_lib import BGENInformation
from general_utilities.job_management.command_executor import CommandExecutor, DockerMount
from general_utilities.linear_model.staar_model import staar_null, staar_genes

# from import_utils.import_lib import TarballType

test_data_dir = Path(__file__).parent / "test_data"
linear_model_test_data_dir = test_data_dir / "linear_model/"

bgen_dict = {'chr1_chunk1': BGENInformation(index= InputFileHandler(test_data_dir / 'chr1_chunk1.bgen.bgi'),
                                            bgen= InputFileHandler(test_data_dir / 'chr1_chunk1.bgen'),
                                            sample= InputFileHandler(test_data_dir / 'chr1_chunk1.sample'),
                                            vep= InputFileHandler(test_data_dir / 'chr1_chunk1.vep.tsv.gz'),
                                            vepidx= InputFileHandler(test_data_dir / 'chr1_chunk1.vep.tsv.gz.tbi')),
             'chr1_chunk2': BGENInformation(index= InputFileHandler(test_data_dir / 'chr1_chunk2.bgen.bgi'),
                                            bgen= InputFileHandler(test_data_dir / 'chr1_chunk2.bgen'),
                                            sample= InputFileHandler(test_data_dir / 'chr1_chunk2.sample'),
                                            vep= InputFileHandler(test_data_dir / 'chr1_chunk2.vep.tsv.gz'),
                                            vepidx= InputFileHandler(test_data_dir / 'chr1_chunk2.vep.tsv.gz.tbi')),
             'chr1_chunk3': BGENInformation(index= InputFileHandler(test_data_dir / 'chr1_chunk3.bgen.bgi'),
                                            bgen= InputFileHandler(test_data_dir / 'chr1_chunk3.bgen'),
                                            sample= InputFileHandler(test_data_dir / 'chr1_chunk3.sample'),
                                            vep= InputFileHandler(test_data_dir / 'chr1_chunk3.vep.tsv.gz'),
                                            vepidx= InputFileHandler(test_data_dir / 'chr1_chunk3.vep.tsv.gz.tbi'))}


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

def test_staar_null(tmp_path, phenofile):

    # Make sure the Docker image can see tmp_path via mounting
    mounts = [DockerMount(tmp_path, Path('/test/'))]
    test_executor = CommandExecutor('egardner413/mrcepid-burdentesting:latest', docker_mounts=mounts)

    # cmd_exector requires all mounted files to be in the same dir (here that means the tmp_path)
    tmp_matrix = tmp_path / 'duat_matrix.sparseGRM.mtx'
    tmp_samples = tmp_path / 'duat_matrix.sparseGRM.mtx.sampleIDs.txt'
    shutil.copy(Path(linear_model_test_data_dir / 'duat_matrix.sparseGRM.mtx'), tmp_matrix)
    shutil.copy(Path(linear_model_test_data_dir / 'duat_matrix.sparseGRM.mtx.sampleIDs.txt'), tmp_samples)

    model_path = staar_null(phenofile=phenofile, phenotype='phenotype',
                            is_binary=False, ignore_base=False,
                            found_quantitative_covariates=[], found_categorical_covariates=['batman'],
                            sex=2,
                            sparse_kinship_file=tmp_matrix,
                            sparse_kinship_samples=tmp_samples,
                            cmd_executor=test_executor)

    # We can't really test the contents of the model since it is R .rds format, but we can check that it was created.
    # Yes, I have tried to install an rds reader in python, but none of them work without installing non-uv managed
    # packages. We can check the validity of the model once we run association tests.
    assert model_path.exists()

def test_build_staar_gene(tmp_path, phenofile):

    # Make sure the Docker image can see tmp_path via mounting
    mounts = [DockerMount(tmp_path, Path('/test/'))]
    test_executor = CommandExecutor('egardner413/mrcepid-burdentesting:latest', docker_mounts=mounts)

    # cmd_exector requires all mounted files to be in the same dir (here that means the tmp_path)
    tmp_matrix = tmp_path / 'duat_matrix.sparseGRM.mtx'
    tmp_samples = tmp_path / 'duat_matrix.sparseGRM.mtx.sampleIDs.txt'
    shutil.copy(Path(linear_model_test_data_dir / 'duat_matrix.sparseGRM.mtx'), tmp_matrix)
    shutil.copy(Path(linear_model_test_data_dir / 'duat_matrix.sparseGRM.mtx.sampleIDs.txt'), tmp_samples)

    model_path = staar_null(phenofile=phenofile, phenotype='phenotype',
                            is_binary=False, ignore_base=False,
                            found_quantitative_covariates=[], found_categorical_covariates=['batman'],
                            sex=2,
                            sparse_kinship_file=tmp_matrix,
                            sparse_kinship_samples=tmp_samples,
                            cmd_executor=test_executor)

    staar_genes(model_path, )
