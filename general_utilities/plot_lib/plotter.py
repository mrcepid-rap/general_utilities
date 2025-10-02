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
        """Run an R script, typically an R script that generates some sort of plot.

        This method python-izes the required inputs to run an R-script via Docker. It takes an R script as a
        Traversable object.

        :param r_script: An Rscript as a Traversable object. This is done as the Rscript should be within a python
            package implemented as part of some plotter and is thus found via the importlib.resources package RATHER
            than through the typical pathlib functionality.
        :param options: A List of Input options for the script.
        :param out_path: The location of the resulting SINGLE output from this script. This parameter has no effect on
            running the script and is provided as a convenience for running. i.e., this output must be set somewhere
            independently in the scripts I/O.
        :return: out_path
        """

        options = [f'{opt}' for opt in options]  # have to convert all opts to strings
        options = " ".join(options)
        plot_cmd = f'Rscript {r_script} {options}'

        self._cmd_executor.run_cmd_on_docker(plot_cmd)

        return out_path
