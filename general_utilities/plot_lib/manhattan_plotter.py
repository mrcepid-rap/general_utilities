import gzip
from pathlib import Path
from typing import List

import pandas as pd
from importlib_resources import files

from general_utilities.job_management.command_executor import CommandExecutor
from general_utilities.plot_lib.cluster_plotter import ClusterPlotter


class ManhattanPlotter(ClusterPlotter):

    def __init__(self, cmd_executor: CommandExecutor, results_table: pd.DataFrame,
                 chrom_column: str, pos_column: str, alt_column: str, id_column: str, p_column: str, csq_column: str,
                 maf_column: str, gene_symbol_column: str, test_name: str = None, sig_threshold: float = 1E-8,
                 suggestive_threshold: float = None, clumping_distance: int = 250000, maf_cutoff: float = 0.001,
                 label_qq: bool = True):
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
        :param suggestive_threshold: Suggestive p.value threshold to cluster variants at. Defaults to None
        :param clumping_distance: Distance to clump variants at. Defaults to 250kbp
        :param maf_cutoff: MAF cutoff to filter variants at. Defaults to 0.001
        :param label_qq: Label the QQ plot with gene names. Defaults to True
        """

        super().__init__(cmd_executor, results_table, chrom_column, pos_column, alt_column, id_column,
                         p_column, csq_column, maf_column, gene_symbol_column, test_name,
                         sig_threshold, suggestive_threshold, clumping_distance)

        self._maf_cutoff = maf_cutoff
        self._suggestive_threshold = suggestive_threshold
        self._label_qq = label_qq
        self._write_plot_table()
        self._write_index_variant_table()

    def _write_plot_table(self):

        query = f'{self._maf_column} >= {self._maf_cutoff}'
        if self._test_name:
            self._plot_table_path = Path(f'current_manh.{self._test_name}.tsv.gz')
            query += f' & TEST == "{self._test_name}"'
        else:
            self._plot_table_path = Path(f'current_manh.tsv.gz')

        self._results_table.query(query).to_csv(gzip.open(self._plot_table_path, 'wt'),
                                                sep='\t', index=False)

    def _write_index_variant_table(self):

        if self._test_name:
            self._index_table_path = Path(f'current_index.{self._test_name}.tsv.gz')
        else:
            self._index_table_path = Path(f'current_index.tsv.gz')

        query = f'{self._maf_column} >= {self._maf_cutoff}'
        self.get_index_variant_table().query(query).to_csv(self._index_table_path, sep='\t', index=False)

    def plot(self) -> List[Path]:

        final_plots = []

        r_script = files('general_utilities.plot_lib.R_resources').joinpath('manhattan_plotter.R')

        # r_script = files('general_utilities.plot_lib.R_resources').joinpath('manhattan_plotter.R')
        # Add something to invert the plot... if (curr_test == paste0('ADD-INT_', interaction_var)) {

        # Do plotting
        output_plot = Path('manhattan_plot.png')
        output_plot.touch()
        options = [f'{self._plot_table_path}', f'{self._index_table_path}', f'{output_plot.absolute()}', self._p_column, self._test_name,
                   self._sig_threshold, self._suggestive_threshold, self._label_qq]
        final_plots.append(self._run_R_script(r_script, options, output_plot))

        return final_plots

    def get_data(self) -> List[Path]:

        return []
