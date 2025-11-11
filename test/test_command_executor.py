from pathlib import Path
from unittest.mock import patch

import pytest

from general_utilities.job_management.command_executor import CommandExecutor
from general_utilities.job_management.command_executor import build_default_command_executor


@pytest.mark.parametrize("cmd,expected_mounts", [
    # No paths
    ("plink2 --out plink_out", [Path.cwd()]),
    # Simple relative output path
    ("plink2 --out ./plink_out", [Path.cwd()]),
    # Nested output path with no trailing slash
    ("plink2 --out results/assoc/output", [Path.cwd() / "results/assoc"]),
    # Absolute path output
    ("plink2 --out /tmp/myrun/plink_out", [Path("/tmp/myrun")]),
    # Mixed input/output
    ("plink2 --bfile input/genotypes --out out/plink_run", [
        Path.cwd() / "input",
        Path.cwd() / "out"
    ]),
    # Output to root (edge case — unlikely but testable)
    ("plink2 --out /plink_global", [Path("/")]),
    # Non-existing output dir in relative path
    ("plink2 --out nonexistdir/output_file", [Path.cwd() / "nonexistdir"]),
    # Current dir explicit
    ("plink2 --out ./out.txt", [Path.cwd()]),
    # Dot-relative input
    ("plink2 --bfile ./data/inputfile", [Path.cwd() / "data"]),
    # Numeric argument (should not be treated as path)
    ("plink2 --threads 8", []),
    # File extension with no slash
    ("plink2 --out myresults.txt", [Path.cwd()]),
    # Weird flag-value pair
    ("plink2 -prefix ./out/abc", [Path.cwd() / "out"]),
    # Symbolic (./..) paths
    ("plink2 --out ../outside_dir/plink", [(Path.cwd() / ".." / "outside_dir").resolve()])
])
def test_docker_mount_detection(cmd, expected_mounts):
    captured_mounts = []

    # Patch the low-level runner so Docker doesn't actually run
    with patch.object(CommandExecutor, "run_cmd", return_value=0) as mock_run:
        # Capture mounts by patching _get_dockermount_for_file
        def fake_get_dockermount_for_file(self, path, safe_mount_point):
            captured_mounts.append(path)
            # Return dummy DockerMount and file path
            from general_utilities.job_management.command_executor import DockerMount
            return DockerMount(path, Path("/mnt/host_cwd") / path.name), Path("/mnt/host_cwd") / path.name

        with patch.object(CommandExecutor, "_get_dockermount_for_file", new=fake_get_dockermount_for_file):
            executor = CommandExecutor(docker_image="dummy/image")
            executor.run_cmd_on_docker(cmd)

    # Convert expected_mounts and captured_mounts to sets of resolved Paths
    expected = set(p.resolve() for p in expected_mounts)
    actual = set(p.resolve() for p in captured_mounts)

    assert expected <= actual, f"Expected mounts: {expected}, but got: {actual}"


def test_plink_creates_output(tmp_path):
    # Use plink2 --dummy to simulate an output
    output_prefix = "plink_out"
    output_log = Path("plink_out.log")

    cmd = f"plink2 --dummy 1 10 0.01 --out {output_prefix}"

    executor = build_default_command_executor()
    exit_code = executor.run_cmd_on_docker(cmd)

    assert exit_code == 0, "PLINK2 exited with error"
    assert Path(output_prefix + ".pgen").exists(), f"Expected output file not found: {output_prefix + '.pgen'}"
    assert Path(output_prefix + ".psam").exists(), f"Expected output file not found: {output_prefix + '.psam'}"
    assert Path(output_prefix + ".pvar").exists(), f"Expected output file not found: {output_prefix + '.pvar'}"
    assert output_log.exists(), f"Expected output file not found: {output_log}"

    # remove the files we created
    output_log.unlink()
    Path(output_prefix + ".pgen").unlink()
    Path(output_prefix + ".psam").unlink()
    Path(output_prefix + ".pvar").unlink()
