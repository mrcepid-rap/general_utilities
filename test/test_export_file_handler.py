from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from dxpy import DXFile

from general_utilities.import_utils.file_handlers.export_file_handler import ExportFileHandler, \
    Platform
from general_utilities.import_utils.file_handlers.input_file_handler import FileType


@pytest.mark.parametrize(
    "uname_value, gcp_check, expected_platform",
    [
        ("job-abcdefghijklmnopqrstuvwx", False, Platform.DX),
        ("some-hostname", True, Platform.GCP),
        ("some-hostname", False, Platform.LOCAL),
    ],
)
def test_platform_detection_parametrized(uname_value, gcp_check, expected_platform):
    """
    Test the platform detection logic in ExportFileHandler._detect_platform method.
    """
    handler = ExportFileHandler()

    with patch.object(handler, "_detect_platform_uname", return_value=uname_value), \
            patch.object(handler, "_is_running_on_gcp_vm", return_value=gcp_check):
        platform = handler._detect_platform()
        assert platform == expected_platform


@pytest.mark.parametrize(
    "input_data, expected_structure",
    [
        ("/path/to/file1.bcf", "converted-file1.bcf"),  # single file string input
        ([Path("/path/to/file1.bcf"), Path("/path/to/file2.bcf")], ["converted-file1.bcf", "converted-file2.bcf"]),
        # list of Paths
        (
                {"out1": "/path/to/file1.bcf", "out2": ["/path/to/file2.bcf", "/path/to/file3.bcf"]},
                {"out1": "converted-file1.bcf", "out2": ["converted-file2.bcf", "converted-file3.bcf"]},
        ),  # dict with mixed inputs
    ],
)
def test_export_files_dnaxexus_mock_upload(input_data, expected_structure):
    """
    Test the export_files method of ExportFileHandler for DNANexus platform.

    This test mocks the _convert_file_to_dxlink method to simulate file conversion without actual DNANexus calls.
    Primarily want to test whether the method correctly processes different input types (so that the output matches the expected structure).
    """
    handler = ExportFileHandler()
    handler.platform = Platform.DX  # Force DNAnexus platform for the test

    # Patch _convert_file_to_dxlink to return a mock string based on filename
    def mock_convert(file):
        return f"converted-{Path(file).name}"

    with patch.object(handler, "_convert_file_to_dxlink", side_effect=mock_convert) as mock_method:
        result = handler.export_files(input_data)

        # Check the mock conversion was called expected number of times
        if isinstance(input_data, dict):
            expected_calls = []
            for v in input_data.values():
                if isinstance(v, list):
                    expected_calls.extend([Path(f).name for f in v])
                else:
                    expected_calls.append(Path(v).name)
            actual_calls = [Path(call[0]).name for call, _ in mock_method.call_args_list]
            assert sorted(actual_calls) == sorted(expected_calls)
        elif isinstance(input_data, list):
            actual_calls = [Path(call[0]).name for call, _ in mock_method.call_args_list]
            expected_calls = [Path(f).name for f in input_data]
            assert sorted(actual_calls) == sorted(expected_calls)
        else:
            # single call
            mock_method.assert_called_once_with(input_data)

        print(result)

        # Verify output matches expected_structure
        assert result == expected_structure


@pytest.mark.parametrize(
    "input_obj, file_type, expected_generate_call",
    [
        (MagicMock(spec=DXFile), FileType.DNA_NEXUS_FILE, True),  # still wrapped via handler
        (Path("/fake/path.txt"), FileType.LOCAL_PATH, True),
        ("fake_path.txt", FileType.LOCAL_PATH, True),
        ({"file": Path("myfile.txt"), "delete_on_upload": False}, FileType.LOCAL_PATH, True),
    ],
)
def test_convert_file_to_dxlink_parametrized(input_obj, file_type, expected_generate_call):
    """
    Test the _convert_file_to_dxlink method of ExportFileHandler with various input types.

    This method should handle different input file types and return a DX link.
    """
    handler = ExportFileHandler()
    mock_linked_file = MagicMock(spec=DXFile)

    with patch('general_utilities.import_utils.file_handlers.export_file_handler.generate_linked_dx_file',
               return_value=mock_linked_file) as mock_generate, \
            patch('general_utilities.import_utils.file_handlers.export_file_handler.dxpy.dxlink',
                  return_value="dxlink_object") as mock_dxlink, \
            patch('general_utilities.import_utils.file_handlers.input_file_handler.InputFileHandler.get_file_type',
                  return_value=file_type), \
            patch('general_utilities.import_utils.file_handlers.input_file_handler.InputFileHandler.get_file_handle',
                  return_value=input_obj), \
            patch('general_utilities.import_utils.file_handlers.input_file_handler.InputFileHandler.__init__',
                  return_value=None):

        # ensure handler instance has required attributes
        with patch.object(handler, '_convert_file_to_dxlink', wraps=handler._convert_file_to_dxlink) as wrapped:
            result = handler._convert_file_to_dxlink(input_obj)

        assert result == "dxlink_object"

        if expected_generate_call:
            mock_generate.assert_called_once_with(input_obj)
            mock_dxlink.assert_called_once_with(mock_linked_file)
        else:
            mock_generate.assert_not_called()
            mock_dxlink.assert_called_once_with(input_obj)
