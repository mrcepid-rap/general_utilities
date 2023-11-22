import pandas as pd

from typing import Dict, Tuple, List
from pathlib import Path

from general_utilities.import_utils.import_lib import BGENInformation
from general_utilities.job_management.command_executor import CommandExecutor
from general_utilities.plot_lib.cluster_plotter import ClusterPlotter


class ManhattanPlotter(ClusterPlotter):

    def __init__(self, cmd_executor: CommandExecutor, results_table: pd.DataFrame, genetic_data: Dict[str, BGENInformation],
                 chrom_column: str, pos_column: str, alt_column: str, id_column: str, p_column: str, csq_column: str,
                 maf_column: str, gene_symbol_column: str):
        super().__init__(cmd_executor, results_table, genetic_data, chrom_column, pos_column, alt_column, id_column,
                         p_column, csq_column, maf_column, gene_symbol_column)

    def plot(self) -> Tuple[List[Path], List[Path]]:

        pass
