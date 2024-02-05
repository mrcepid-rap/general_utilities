import os
import re
import dxpy
import pytest

from pathlib import Path

test_folder = Path(os.getenv('TEST_DIR'))
cwd = Path(os.getcwd())

# A big note to users of this test: We have to do this to get the subjob test modules to run properly on DNANexus.
# The actual methods called by this test are located in the general_utilities package. This is because there is no
# way ( as far as I can tell) to load a module not instantiated at startup (e.g., with github like for actual
# modules) when running subjobs. This means if something breaks in this 'test' it is likely part of the github repo
# and NOT part of this testing directory.
from general_utilities.job_management.subjob_test.subjob_test import subjob_testing


@pytest.mark.parametrize(
    argnames=['tabix_file', 'dereference_outputs'],
    argvalues=zip(['file-GZzx098J0zVxY2JXgBk5B16X', 'file-GZzx098J0zVxY2JXgBk5B16X'],
                  [True, False])
)
def test_subjob_build(tabix_file, dereference_outputs):

    tabix_dxfile = dxpy.DXFile(dxid=tabix_file)

    output_files = subjob_testing(tabix_dxfile, dereference_outputs)

    assert len(output_files) == 22

    found_chromosomes = []

    if dereference_outputs:
        for output_file in output_files:

            curr_file = dxpy.DXFile(output_file)
            file_name = curr_file.describe()['name']
            name_match = re.match('chr(\\d+).tsv', file_name)

            assert name_match

            if name_match:
                found_chromosomes.append(name_match.group(1))
    else:

        for output_files in output_files:
            print(output_files)

    for chrom in range(1,23):
        assert str(chrom) in found_chromosomes
