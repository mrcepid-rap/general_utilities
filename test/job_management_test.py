import os
import re
import dxpy
import pytest

from pathlib import Path

test_folder = Path(os.getenv('TEST_DIR'))
cwd = Path(os.getcwd())

# We have to do this to get the subjob test modules to run properly on DNANexus
from general_utilities.job_management.subjob_test.subjob_test import test_subjob


@pytest.mark.parametrize(
    argnames=['tabix_file'],
    argvalues=zip(['file-GZzx098J0zVxY2JXgBk5B16X'])
)
def test_subjob_build(tabix_file):

    tabix_dxfile = dxpy.DXFile(dxid=tabix_file)

    output_files = test_subjob(tabix_dxfile)

    assert len(output_files) == 22

    found_chromosomes = []

    for output_file in output_files:

        curr_file = dxpy.DXFile(output_file)
        file_name = curr_file.describe()['name']
        name_match = re.match('chr(\\d+).tsv', file_name)

        assert name_match

        if name_match:
            found_chromosomes.append(name_match.group(1))

    for chrom in range(1,23):
        assert str(chrom) in found_chromosomes
