import os
import pytest

from typing import List
from pathlib import Path
from general_utilities import association_resources

test_folder = os.getenv('TEST_DIR')


@pytest.mark.parametrize(
    argnames=['is_snp_tar', 'is_gene_tar', 'chromosome', 'output_array', 'expected_exception'],
    argvalues=zip([False, True, False, False, True, False],
                  [False, False, True, False, True, False],
                  [None, None, None, '1', '', 'A'],
                  [['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X'],
                   ['SNP'], ['GENE'], ['1'], [], []],
                  [None, None, None, None, ValueError, ValueError])
)
def test_get_chromosomes(is_snp_tar: bool, is_gene_tar: bool, chromosome: str,
                         output_array: List[str], expected_exception: Exception):
    """Test association_testing get_chromosomes

    This is a straightforward method that returns a list of chromosomes to perform analysis on. We are running 6 tests:

    1. Default
    2. SNP tar
    3. Gene tar
    4. Specific chromosome (1)
    5. Both SNP / Gene (ERROR)
    6. Wrong Chromosome (ERROR)

    :param is_snp_tar: Is the current analysis processing a collapsed SNP fileset?
    :param is_gene_tar: Is the current analysis processing a collapsed GENE-list fileset?
    :param chromosome: Do we want to analyse a single chromosome?
    :param output_array: The expected output array from these specific inputs.
    :param expected_exception: Expected error for the current input parameters
    """

    if expected_exception is None:
        method_output = association_resources.get_chromosomes(is_snp_tar, is_gene_tar, chromosome)
        assert method_output == output_array
    else:
        with pytest.raises(expected_exception):
            association_resources.get_chromosomes(is_snp_tar, is_gene_tar, chromosome)


@pytest.mark.parametrize(
    argnames=['upload_type'],
    argvalues=zip([str, Path])
)
def test_generate_linked_dx_file(upload_type: type):
    """Test generate_linked_dx_file

    generate_linked_dx_file will just upload a file to the local container and wait for a further 'dxpy.dxlink()'
    call and applet termination to actually upload a given file to the project. We just want to test that (1) the
    file upload actually happens and (2) the file is deleted from the local instance to save space.

    :param upload_type: generate_linked_dx_file can take either a str / Path representation of a file.
    """

    test_path = Path('test.txt')
    with test_path.open('w') as test_file:
        test_file.write('This is a test file to upload to the DNANexus platform...\n')

    if upload_type == str:
        uploaded_file = association_resources.generate_linked_dx_file(test_path.name)
    elif upload_type == Path:
        uploaded_file = association_resources.generate_linked_dx_file(test_path)

    assert test_path.exists() == False and uploaded_file.describe(fields={'name': True})['name'] == 'test.txt'


def test_process_bgen_file():
    """Test process_bgen_file() in association_resources.

    TODO: Generate actual test data that we can use for this test...


    """
    pass


def test_build_transcript_table():
    """Make sure the transcript table is loaded properly with correct columns and formatting.

    This method isn't of too much concern, but want to at least test that the table loads, the columns are in the
    correct order, and the total number of transcripts is 18,645
    """

    transcripts = association_resources.build_transcript_table()

    assert transcripts.columns.to_list() == ['chrom', 'start', 'end', 'ENSG', 'MANE', 'transcript_length', 'SYMBOL', 'CANONICAL', 'BIOTYPE', 'cds_length', 'coord', 'manh.pos']
    assert len(transcripts) == 18645


transcripts = association_resources.build_transcript_table()


@pytest.mark.parametrize(argnames=['gene_id', 'test_error', 'coord'],
                         argvalues=zip(['ENST00000361132', 'AARS1', 'ENST00000381394', 'ENST00000000001', 'ENST00000619989', 'ABCDEF1'],
                                       [None, None, KeyError, KeyError, ValueError, ValueError],
                                       ['chr5:180100795-180209211', 'chr16:70251983-70289707', None, None, None, None]
                                       ))
def test_get_gene_id(gene_id: str, test_error: Exception, coord: str):
    """Make sure the transcript table contains the information we need and that it can be extracted properly

    The first two inputs to :param gene_id: are valid – the others test various fail categories:

    1. A valid ENST ID

    2. A valid HGNC Gene Symbol

    3. ENST00000381394 – A valid ENST ID that should not be actually be in transcripts because it is a failure
        (has 0 variants)

    4. An invalid ENST ID

    5. An ENST with multiple entries

    6. A fake gene name

    :param gene_id: HGNC Symbol or a valid ENST ID (typically MANE)
    :param test_error: Should this geneID throw an error
    :param coord: Coorect coordinate for this gene
    """

    if test_error is None:
        gene_info = association_resources.get_gene_id(gene_id=gene_id, transcripts_table=transcripts)
        assert gene_info

    else:
        with pytest.raises(test_error):
            association_resources.get_gene_id(gene_id=gene_id, transcripts_table=transcripts)








