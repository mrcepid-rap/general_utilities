import gzip

import pandas as pd

from typing import List
from pathlib import Path

from importlib_resources import files

from general_utilities.job_management.command_executor import CommandExecutor
from general_utilities.plot_lib.cluster_plotter import ClusterPlotter


class ManhattanPlotter(ClusterPlotter):

    def __init__(self, cmd_executor: CommandExecutor, results_table: pd.DataFrame,
                 chrom_column: str, pos_column: str, alt_column: str, id_column: str, p_column: str, csq_column: str,
                 maf_column: str, gene_symbol_column: str, test_name: str = None, sig_threshold: float = 1E-6,
                 clumping_distance: int = 250000, maf_cutoff: float = 0.001):
        """Plot a Manhattan plot of some genetic result.

        :param cmd_executor: A CommandExecutor to run commands via the command line or provided Docker image
        :param results_table: A table from _some_ genetic association pipeline, typically from imputed markers.
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
        super().__init__(cmd_executor, results_table, chrom_column, pos_column, alt_column, id_column,
                         p_column, csq_column, maf_column, gene_symbol_column, test_name, sig_threshold,
                         clumping_distance)

        self._maf_cutoff = maf_cutoff
        self._write_plot_table()
        self._write_index_variant_table()

    def _write_plot_table(self):

        query = f'{self._maf_column} >= {self._maf_cutoff}'
        if self._test_name:
            self._plot_table_path = Path(f'current_manh.{self._test_name}.tsv.gz')
            query += f' & TEST == "{self._test_name}"'
            self._results_table.query(query).to_csv(gzip.open(self._plot_table_path, 'wt'),
                                                    sep='\t', index=False)
        else:
            self._plot_table_path = Path(f'current_manh.tsv.gz')
            self._results_table.query(query).to_csv(gzip.open(self._plot_table_path, 'wt'),
                                                    sep='\t', index=False)

    def _write_index_variant_table(self):

        if self._test_name:
            self._index_table_path = Path(f'current_index.{self._test_name}.tsv.gz')
            self.get_index_variant_table().to_csv(self._index_table_path, sep='\t', index=False)
        else:
            self._index_table_path = Path(f'current_index.tsv.gz')
            self.get_index_variant_table().to_csv(self._index_table_path, sep='\t', index=False)

    def plot(self) -> List[Path]:

        final_plots = []

        r_script = files('general_utilities.plot_lib.R_resources').joinpath('manhattan_plotter.R')

        # Add something to invert the plot... if (curr_test == paste0('ADD-INT_', interaction_var)) {

        # Do plotting
        options = [f'/test/{self._plot_table_path}', f'/test/{self._index_table_path}', f'/test/mean_chr_pos.tsv',
                   self._p_column]
        final_plots.append(self._run_R_script(r_script, options, Path('manhattan_plot.png')))

        return final_plots

    def get_data(self) -> List[Path]:

        return []
