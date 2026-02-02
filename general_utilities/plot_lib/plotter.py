from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Any

from importlib_resources.abc import Traversable

from general_utilities.job_management.command_executor import CommandExecutor
from general_utilities.mrc_logger import MRCLogger


class Plotter(ABC):
    """An Interface for classes that generate plots via R's ggplot2 library.

    This interface ensures that implementing classes always:

    1. Plot something via the :func:`plot()` method

    2. Have a built-in method to run R-scripts without having to re-implement the same rough functionality via the
    :func:`self._run_R_script()` method.

    :param cmd_executor: A CommandExecutor to run commands via the command line or provided Docker image
    """

    def __init__(self, cmd_executor: CommandExecutor):
        self._logger = MRCLogger(__name__).get_logger()
        self._cmd_executor = cmd_executor

    @abstractmethod
    def plot(self) -> List[Path]:
        """Abstract method to implement some form of plotting.

        :return: A list of Paths of plots.
        """
        pass

    @abstractmethod
    def get_data(self) -> List[Path]:
        """Abstract method to return some sort of data.

        :return: A list of Paths of data tables.
        """
        pass

    def _run_R_script(self, r_script: Traversable, options: List[Any], out_path: Path) -> Path:
        """
        Run an R script (typically a plotting script) inside Docker,
        passing all arguments and the output path explicitly.
        """

        # Convert all args to strings and ensure absolute paths
        r_script = Path(str(r_script)).resolve()
        out_path = Path(out_path).resolve()

        # Append the output path to the end of the options list
        args = [str(opt) for opt in options] + [str(out_path)]

        # Build the full Rscript command
        plot_cmd = f"Rscript {r_script} {' '.join(args)}"

        # Optional: log it for debug
        self._logger.info(f"Running R plotting command:\n{plot_cmd}")

        # Run via Docker
        self._cmd_executor.run_cmd_on_docker(plot_cmd)

        return out_path
