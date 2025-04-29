from pathlib import Path

import dxpy
import pytest

from general_utilities.import_utils.file_handlers.input_file_handler import InputFileHandler, FileType


@pytest.mark.parametrize(
    "input_file, expected_file_type, expected_exception",
    [
        ("file-Fx2x21QJ06f47gV73kZPjkQQ", FileType.DNA_NEXUS_FILE, None),
        ("project-GbZqJpQJ9bvfF97z25B9Gkjv:file-Fx2x21QJ06f47gV73kZPjkQQ", FileType.DNA_NEXUS_FILE, None),
        ("project-Fx2x0fQJ06KfqV7Y3fFZq1jp:/H. Sapiens - GRCh38 with alt contigs - hs38DH/hs38DH.fa.fai",
         FileType.DNA_NEXUS_FILE, None),
        ("test_data/transcripts.tsv.gz", FileType.LOCAL_PATH, None),
        (Path("test_data/transcripts.tsv.gz"), FileType.LOCAL_PATH, None),
        ("gs://bucket-name/file-name", FileType.GCLOUD_FILE, None),
        ("invalid-input", None, FileNotFoundError),
    ],
)
def test_input_file_handler_get_file_type(input_file, expected_file_type, expected_exception):
    """
    Test the InputFileHandler class with regards to recognizing filetypes.
    """
    if expected_exception:
        with pytest.raises(expected_exception):
            input_handler = InputFileHandler(input_file)
            input_handler.get_file_type()
    else:
        input_handler = InputFileHandler(input_file)
        assert input_handler.get_file_type() == expected_file_type


@pytest.mark.parametrize(
    "input_file, expected_file_type, expected_exception, expected_file",
    [
        ("file-Fx2x21QJ06f47gV73kZPjkQQ", FileType.DNA_NEXUS_FILE, None, Path("hs38DH.fa.fai")),
        ("file-Fx2x21QJ06f47gV73kZPjkQQGXXX", FileType.DNA_NEXUS_FILE, dxpy.DXError, Path("hs38DH.fa.fai")),
        ("project-Fx2x0fQJ06KfqV7Y3fFZq1jp:file-Fx2x21QJ06f47gV73kZPjkQQ", FileType.DNA_NEXUS_FILE, None,
         Path("hs38DH.fa.fai")),
        ("project-Fx2x0fQJ06KfqV7Y3fFZq1jpXXX:file-Fx2x21QJ06f47gV73kZPjkQQ", FileType.DNA_NEXUS_FILE, dxpy.exceptions.InvalidInput,
         Path("hs38DH.fa.fai")),
        ("project-Fx2x0fQJ06KfqV7Y3fFZq1jp:file-Fx2x21QJ06f47gV73kZPjkQQXXX", FileType.DNA_NEXUS_FILE, TypeError,
         Path("hs38DH.fa.fai")),
        ('H. Sapiens - GRCh38 with alt contigs - hs38DH/hs38DH.fa.fai', FileType.DNA_NEXUS_FILE, FileNotFoundError,
         Path("hs38DH.fa.fai")),
        ('H. Sapiens - GRCh38 with alt contigs - hs38DHXXX/hs38DH.fa.fai', FileType.DNA_NEXUS_FILE, FileNotFoundError,
         Path("hs38DH.fa.fai")),
        ("project-Fx2x0fQJ06KfqV7Y3fFZq1jp:/H. Sapiens - GRCh38 with alt contigs - hs38DH/hs38DH.fa.fai",
         FileType.DNA_NEXUS_FILE, None,
         Path("hs38DH.fa.fai")),
        ("project-Fx2x0fQJ06KfqV7Y3fFZq1jp:/H. Sapiens - GRCh38 with alt contigs - hs38DHXXX/hs38DH.fa.fai",
         FileType.DNA_NEXUS_FILE, FileNotFoundError,
         Path("hs38DH.fa.fai")),
        ("project-Fx2x0fQJ06KfqV7Y3fFZq1jp/H. Sapiens - GRCh38 with alt contigs - hs38DH/hs38DH.fa.fai",
         FileType.DNA_NEXUS_FILE, ValueError,
         Path("hs38DH.fa.fai")),
        ("project-Fx2x0fQJ06KfqV7Y3fFZq1jp:H. Sapiens - GRCh38 with alt contigs - hs38DH/hs38DH.fa.fai",
         FileType.DNA_NEXUS_FILE, None,
         Path("hs38DH.fa.fai")),
        ("test_data/transcripts.tsv.gz", FileType.LOCAL_PATH, None, Path("transcripts.tsv.gz")),
    ],
)
def test_dna_nexus_files_explicitly(input_file, expected_file_type, expected_exception, expected_file):
    """
    Test the InputFileHandler class with regards to recognizing filetypes and downloading them.
    """
    if expected_exception:
        with pytest.raises(expected_exception):
            input_handler = InputFileHandler(input_file)
            input_handler.get_file_type()
    else:
        input_handler = InputFileHandler(input_file)
        print(input_handler.get_file_handle())
        assert input_handler.get_file_type() == expected_file_type

        assert expected_file.exists()

        # remove the file so we can run the test properly
        expected_file.unlink()
