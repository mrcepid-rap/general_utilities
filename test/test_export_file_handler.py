from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from dxpy import DXFile

from general_utilities.import_utils.file_handlers.export_file_handler import ExportFileHandler, \
    Platform
from general_utilities.import_utils.file_handlers.input_file_handler import FileType


@pytest.mark.parametrize(
    "input_data, expected_result",
    [
        ("/path/to/file1.bcf", "converted-file1.bcf"),  # single file string input
        (Path("/path/to/file2.bcf"), "converted-file2.bcf"),  # single Path input
        ([Path("/path/to/file1.bcf"), Path("/path/to/file2.bcf")],
         ["converted-file1.bcf", "converted-file2.bcf"]),  # list of Paths
        (["/path/to/file1.bcf", "/path/to/file2.bcf"],
         ["converted-file1.bcf", "converted-file2.bcf"]),  # list of strings
    ],
)
def test_export_files_dnaxexus_mock_upload(input_data, expected_result):
    handler = ExportFileHandler()
    handler._platform = Platform.DX

    def mock_convert(file):
        return f"converted-{Path(file).name}"

    with patch.object(handler, "_convert_file_to_dxlink", side_effect=mock_convert) as mock_method:
        result = handler.export_files(input_data)

        if isinstance(input_data, list):
            expected_calls = [Path(f).name for f in input_data]
            actual_calls = [Path(call.args[0]).name for call in mock_method.call_args_list]
            assert sorted(actual_calls) == sorted(expected_calls)
        else:
            mock_method.assert_called_once_with(input_data)

        assert result == expected_result


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
