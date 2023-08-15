from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Tuple, Any

from importlib_resources.abc import Traversable

from general_utilities.mrc_logger import MRCLogger
from general_utilities.job_management.command_executor import CommandExecutor, DockerMount, \
    build_default_command_executor


class Plotter(ABC):
    """A class to generate summary plots for genetic markers.

    This class automates the process of clumping genetic variants. This class proceeds in four main steps:

    1. Clustering – Identify clusters of significant variants using `bedtools cluster` at distance set by
        `clumping_distance`

    2. LD Calculation – Calculate r^2 between the identified index variant and all variants ± `clumping_distance`.
        Clumping is performed simply by some distance (specifically, the `clumping_distance` parameter) and selects the
        variant with the highest p. value, independent of any LD structure. This leans heavily on the BGENReader class
        to enable rapid extracting of variant genotypes.

    3. Plot – Create locus zoom plots for each index variant. These plots show log10 p. value ~ genomic coordinate,
        with points colour coded by r^2 (based on Pearson's CC) to the index variant, and shaped based on if they have a
        consequence of missense, PTV, or 'other'.

    Input to this class is a pandas DataFrame, typically from one of the major tools (e.g. BOLT / REGENIE). This table
    MUST have columns indicating:

    Chromosome, Position, alt, ref, some ID, p. value, and consequence

    These names for these columns in the underlying data can all be set when the class is instantiated. There is an
    optional column, 'TEST', that can be provided to further subset variants. This column name is not changeable via
    class inputs as it is very specific and should be unlikely to change.

    :param results_table: A table from _some_ genetic association pipeline, typically from imputed markers.
    :param genetic_data: A dictionary of chromosome keys and GeneticData values pointing to the location of the
        underlying genetic data – MUST be .bgen
    :param chrom_column: The name of the chromosome column in `results_table`
    :param pos_column: The name of the position column in `results_table`
    :param alt_column: The name of the alt column in `results_table`
    :param id_column: The name of the ID column in `results_table`
    :param p_column: The name of the p. value column in `results_table`
    :param csq_column: The name of the consequence annotation column in `results_table`
    :param maf_column: The name of the MAF annotation column in `results_table`
    :param gene_symbol_column: The name of the gene name annotation column in `results_table`
    :param test_name: A specific test to subset from the 'TEST' columns. Relevant to REGENIE outputs
    :param sig_threshold: Significance threshold to cluster variants at. Defaults to 1E-6
    :param clumping_distance: Distance to clump variants at. Defaults to 250kbp
    """

    def __init__(self, cmd_executor: CommandExecutor):

        self._logger = MRCLogger(__name__).get_logger()
        self._cmd_executor = cmd_executor

    @abstractmethod
    def plot(self) -> Tuple[List[Path], List[Path]]:
        pass

    def _run_R_script(self, r_script: Traversable, options: List[Any], out_path: Path) -> Path:

        options = [f'{opt}' for opt in options]  # have to convert all opts to strings
        options = " ".join(options)
        plot_cmd = f'Rscript /scripts/{r_script.name} {options}'

        script_mount = DockerMount(Path(f'{r_script.parent}/'),
                                   Path('/scripts/'))

        self._cmd_executor.run_cmd_on_docker(plot_cmd, docker_mounts=[script_mount])

        return out_path


