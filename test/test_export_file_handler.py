from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from dxpy import DXFile

from general_utilities.import_utils.file_handlers.export_file_handler import ExportFileHandler, \
    Platform, generate_linked_dx_file


@pytest.mark.parametrize(
    "uname_value, gcp_check, expected_platform",
    [
        ("job-abcdefghijklmnopqrstuvwx", False, Platform.DX),
        ("some-hostname", True, Platform.GCP),
        ("some-hostname", False, Platform.LOCAL),
    ],
)
def test_platform_detection_parametrized(uname_value, gcp_check, expected_platform):
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
    "input_obj, is_dxfile, expected_calls",
    [
        (MagicMock(spec=DXFile, dxid="file-123"), True, 0),  # Already DXFile, no calls expected
        (Path("/path/to/file1.txt"), False, 2),              # Normal path triggers 2 calls
        ("string_path.txt", False, 2),                        # String path also triggers calls
    ],
)
def test_convert_file_to_dxlink_parametrized(input_obj, is_dxfile, expected_calls):
    handler = ExportFileHandler()

    with patch('general_utilities.import_utils.file_handlers.export_file_handler.generate_linked_dx_file', return_value="linked_file") as mock_generate, \
         patch('general_utilities.import_utils.file_handlers.export_file_handler.dxpy.dxlink', return_value="dxlink_object") as mock_dxlink:

        result = handler._convert_file_to_dxlink(input_obj)

        print(result)

        if is_dxfile:
            assert result is input_obj
            assert mock_generate.call_count == 0
            assert mock_dxlink.call_count == 0
        else:
            assert result == "dxlink_object"
            assert mock_generate.call_count == 1
            assert mock_generate.call_args[0][0] == input_obj
            assert mock_dxlink.call_count == 1
            assert mock_dxlink.call_args[0][0] == "linked_file"


@pytest.mark.parametrize(
    "bad_input",
    [
        None,
        123,
        45.6,
        [],
        {},
        object(),
    ],
)
def test_convert_file_to_dxlink_failsafe(bad_input):
    handler = ExportFileHandler()

    with pytest.raises(Exception):
        handler._convert_file_to_dxlink(bad_input)
