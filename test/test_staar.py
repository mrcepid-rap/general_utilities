import csv
import shutil
import pytest
import pandas as pd

from math import isnan
from pathlib import Path
from scipy.io import mmwrite

from general_utilities.bgen_utilities.genotype_matrix import generate_csr_matrix_from_bgen
from general_utilities.association_resources import build_transcript_table
from general_utilities.import_utils.file_handlers.input_file_handler import InputFileHandler
from general_utilities.import_utils.import_lib import BGENInformation
from general_utilities.job_management.command_executor import CommandExecutor, DockerMount
from general_utilities.linear_model.staar_model import staar_null, staar_genes, load_staar_genetic_data
from general_utilities.linear_model.proccess_model_output import process_model_outputs
from general_utilities.import_utils.import_lib import TarballType

test_data_dir = Path(__file__).parent / "test_data"
linear_model_test_data_dir = test_data_dir / "linear_model/"

bgen_dict = {'chr1_chunk1': BGENInformation(index= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk1.bgen.bgi'),
                                            bgen= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk1.bgen'),
                                            sample= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk1.sample'),
                                            vep= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk1.vep.tsv.gz'),
                                            vepidx= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk1.vep.tsv.gz.tbi')),
             'chr1_chunk2': BGENInformation(index= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk2.bgen.bgi'),
                                            bgen= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk2.bgen'),
                                            sample= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk2.sample'),
                                            vep= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk2.vep.tsv.gz'),
                                            vepidx= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk2.vep.tsv.gz.tbi')),
             'chr1_chunk3': BGENInformation(index= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk3.bgen.bgi'),
                                            bgen= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk3.bgen'),
                                            sample= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk3.sample'),
                                            vep= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk3.vep.tsv.gz'),
                                            vepidx= InputFileHandler(linear_model_test_data_dir / 'chr1_chunk3.vep.tsv.gz.tbi'))}


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
    """Tests the creation of a STAAR null model using a sparse kinship matrix and covariates."""

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

@pytest.mark.parametrize('unpacked_tarball, expected_genes_path',
                         zip(
                             ('HC_PTV-MAF_001',),
                             (linear_model_test_data_dir / 'expt_genes.PTV.tsv',)
                         ), indirect=['unpacked_tarball'])
def test_load_staar(tmp_path, unpacked_tarball, expected_genes_path: Path):
    """Tests loading the genetic data from a STAAR tarball and compares to expected genes."""

    variant_infos = load_staar_genetic_data(str(unpacked_tarball))

    expected_genes = set()
    with expected_genes_path.open('r') as expected_genes_reader:
        for gene in expected_genes_reader:
            expected_genes.add(gene.rstrip())

    assert variant_infos.keys() == bgen_dict.keys()

    found_genes = set()
    for variant_info in variant_infos.values():
        [found_genes.add(gene) for gene in variant_info.keys()]

    assert len(found_genes.intersection(expected_genes)) == len(expected_genes)


@pytest.mark.parametrize('unpacked_tarball', ('HC_PTV-MAF_001',), indirect=['unpacked_tarball'])
def test_staar_genes_genomewide(tmp_path, phenofile, transcripts_table, unpacked_tarball):
    """Tests running STAAR on a genomewide tarball.

    We test SNP / GENE masks in a separate function as the inputs and required checks are very different.
    """

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

    variant_infos = load_staar_genetic_data(str(unpacked_tarball))

    # If running on a M4 Pro Mac, do NOT set threads above 4 as Docker will likely blow up...

    finished_models = []
    for bgen_prefix, variant_info in variant_infos.items():

        staar_samples = Path(unpacked_tarball.parent / f'{unpacked_tarball.name}.{bgen_prefix}.STAAR.samples_table.tsv')
        staar_variants = Path(unpacked_tarball.parent / f'{unpacked_tarball.name}.{bgen_prefix}.STAAR.variants_table.tsv')
        bgen_info = bgen_dict[bgen_prefix]

        tested_counter = 0

        # Docker fails if too many threads are spawned, so we test 10 genes randomly from each bgen file
        info_iter = iter(variant_info.items())
        while tested_counter < 10:
            gene, gene_info = next(info_iter)

            # The STAAR method does not collapse the genotype matrix, so we need to do that here if using a burden mask
            gene_matrix, _ = generate_csr_matrix_from_bgen(bgen_path=bgen_info['bgen'].get_file_handle(),
                                                           sample_path=bgen_info['sample'].get_file_handle(),
                                                           variant_filter_list=gene_info['vars'],
                                                           chromosome=gene_info['chrom'], start=gene_info['min'],
                                                           end=gene_info['max'],
                                                           should_collapse_matrix=False)

            staar_matrix = tmp_path / f'{gene}.phenotype.STAAR.mtx'
            mmwrite(staar_matrix, gene_matrix)

            # This is because of the bug in the Duat data with duplicated variants
            if gene == 'ENST00000337107':
                continue

            bgen_info['index'].get_file_handle()
            staar_return = staar_genes(staar_null_path=model_path, pheno_name='phenotype', gene=gene, mask_name=unpacked_tarball.name,
                                       staar_matrix=staar_matrix, staar_samples=staar_samples, staar_variants=staar_variants,
                                       out_dir=tmp_path, cmd_executor=test_executor)

            tested_counter += 1

            assert staar_return.ENST == gene
            assert staar_return.mask_name == unpacked_tarball.name
            assert staar_return.pheno_name == 'phenotype'
            assert staar_return.relatedness_correction == False

            if not staar_return.model_run:
                assert staar_return.cMAC <= 2
                assert isnan(staar_return.p_val_O)
                assert isnan(staar_return.p_val_SKAT)
                assert isnan(staar_return.p_val_ACAT)
                assert isnan(staar_return.p_val_burden)

            else:
                assert staar_return.cMAC > 2
                assert not isnan(staar_return.p_val_O)
                assert not isnan(staar_return.p_val_SKAT)
                assert not isnan(staar_return.p_val_ACAT)
                assert not isnan(staar_return.p_val_burden)

            finished_models.append(staar_return)

    assert len(finished_models) == 30

    output_path = tmp_path / f'{unpacked_tarball.name}.genes.STAAR.stats.tsv'
    output_files = process_model_outputs(input_models=finished_models, output_path=output_path,
                                         tarball_type=TarballType.GENOMEWIDE, transcripts_table=transcripts_table)

    assert len(output_files) == 2
    assert output_files[0].exists()
    assert output_files[0].name == f'{unpacked_tarball.name}.genes.STAAR.stats.tsv.gz'
    assert output_files[1].exists()
    assert output_files[1].name == f'{unpacked_tarball.name}.genes.STAAR.stats.tsv.gz.tbi'

    expected_fields = ['ENST', 'chrom', 'start', 'end', 'ENSG', 'MANE', 'transcript_length', 'SYMBOL', 'CANONICAL',
                       'BIOTYPE', 'cds_length', 'coord', 'manh.pos', 'MASK', 'MAF', 'pheno_name', 'n_var', 'cMAC',
                       'n_model', 'model_run', 'relatedness_correction', 'p_val_O', 'p_val_SKAT', 'p_val_burden',
                       'p_val_ACAT']

    test_output = pd.read_csv(output_files[0], sep='\t')
    assert test_output.columns.tolist() == expected_fields
    assert len(test_output) == 30


@pytest.mark.parametrize('unpacked_tarball, tarball_type',
                         zip(
                             ('HC_PTV-MAF_001_GENE','HC_PTV-MAF_001_SNP'),
                             (TarballType.GENE, TarballType.SNP)
                         ), indirect=['unpacked_tarball'])
def test_staar_genes_gene_or_snp(tmp_path, phenofile, transcripts_table, unpacked_tarball, tarball_type):
    """Tests running STAAR on a gene or SNP tarball."""

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

    staar_samples = Path(unpacked_tarball.parent / f'{unpacked_tarball.name}.{tarball_type.name}.STAAR.samples_table.tsv')
    staar_variants = Path(unpacked_tarball.parent / f'{unpacked_tarball.name}.{tarball_type.name}.STAAR.variants_table.tsv')
    staar_matrix = Path(unpacked_tarball.parent / f'{unpacked_tarball.name}.{tarball_type.name}.STAAR.mtx')

    staar_return = staar_genes(staar_null_path=model_path, pheno_name='phenotype', gene=tarball_type.value, mask_name=unpacked_tarball.name,
                               staar_matrix=staar_matrix, staar_samples=staar_samples, staar_variants=staar_variants,
                               out_dir=tmp_path, cmd_executor=test_executor)

    assert staar_return.ENST == tarball_type.value
    assert staar_return.mask_name == unpacked_tarball.name
    assert staar_return.pheno_name == 'phenotype'
    assert staar_return.relatedness_correction == False

    if not staar_return.model_run:
        assert staar_return.cMAC <= 2
        assert isnan(staar_return.p_val_O)
        assert isnan(staar_return.p_val_SKAT)
        assert isnan(staar_return.p_val_ACAT)
        assert isnan(staar_return.p_val_burden)

    else:
        assert staar_return.cMAC > 2
        assert not isnan(staar_return.p_val_O)
        assert not isnan(staar_return.p_val_SKAT)
        assert not isnan(staar_return.p_val_ACAT)
        assert not isnan(staar_return.p_val_burden)

    output_path = tmp_path / f'{unpacked_tarball.name}.{tarball_type.name}.STAAR.stats.tsv'
    output_files = process_model_outputs(input_models=[staar_return], output_path=output_path,
                                         tarball_type=tarball_type, transcripts_table=transcripts_table)
    assert len(output_files) == 1
    assert output_files[0].exists()
    assert output_files[0].name == f'{unpacked_tarball.name}.{tarball_type.name}.STAAR.stats.tsv.gz'

    expected_fields = ['ENST', 'MASK', 'MAF', 'pheno_name', 'n_var', 'cMAC', 'n_model', 'model_run',
                       'relatedness_correction', 'p_val_O', 'p_val_SKAT', 'p_val_burden', 'p_val_ACAT']

    test_output = pd.read_csv(output_files[0], sep='\t')
    assert test_output.columns.tolist() == expected_fields
    assert len(test_output) == 1