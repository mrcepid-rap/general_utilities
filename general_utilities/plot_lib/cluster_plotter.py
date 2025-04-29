import random
from abc import ABC
from pathlib import Path
from typing import Tuple, List

import pandas as pd

from general_utilities.association_resources import get_include_sample_ids
from general_utilities.job_management.command_executor import CommandExecutor
from general_utilities.plot_lib.plotter import Plotter


class ClusterPlotter(Plotter, ABC):
    """This class is an interface for all plot-types that need to determine 'index' variants for plotting purposes.

    ClusterPlotter is itself an implementation of :func:`Plotter()`. The only additional functionality of this class
    is to do variant clumping. Clumping is performed by distance (specified via the `clumping_distance` parameter)
    and selects the variant with the highest p. value, independent of any LD structure.

    Input to this class is a pandas DataFrame, typically from one of the major tools (e.g. BOLT / REGENIE). This table
    MUST have columns indicating:

    Chromosome, Position, alt, ref, some ID, p. value, and consequence

    These names for these columns in the underlying data can all be set when the class is instantiated. There is an
    optional column, 'TEST', that can be provided to further subset variants. This column name is not changeable via
    class inputs as it is very specific and should be unlikely to change.

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

    def __init__(self, cmd_executor: CommandExecutor, results_table: pd.DataFrame,
                 chrom_column: str, pos_column: str, alt_column: str, id_column: str, p_column: str, csq_column: str,
                 maf_column: str, gene_symbol_column: str, test_name: str = None,
                 sig_threshold: float = 1E-6, sugg_threshold: float = None, clumping_distance: int = 250000):

        super().__init__(cmd_executor)

        self._results_table = results_table

        # Query variables
        self._test_name = test_name
        # Need to decide which threshold to use for clumping if both sig and sugg are provided
        # Future Eugene – max() is correct since we are dealing with decimal numbers, not log10(p)...
        self._sig_threshold = sig_threshold
        self._sugg_threshold = sugg_threshold
        self._clumping_distance = clumping_distance

        # Set column name variables
        self._chrom_column = chrom_column
        self._pos_column = pos_column
        self._alt_column = alt_column
        self._id_column = id_column
        self._p_column = p_column
        self._csq_column = csq_column
        self._maf_column = maf_column
        self._gene_symbol_column = gene_symbol_column

        # Get index variants by distance (no LD clumping done)
        self._index_variants = self._cluster_variants()

    def get_index_variant_table(self) -> pd.DataFrame:
        return self._index_variants

    def _cluster_variants(self) -> pd.DataFrame:

        cluster_threshold = self._sig_threshold if self._sugg_threshold is None else max(self._sig_threshold,
                                                                                         self._sugg_threshold)

        if self._test_name is None:
            query = f'{self._p_column} < {cluster_threshold}'
        else:
            query = f'TEST == "{self._test_name}" & {self._p_column} < {cluster_threshold}'

        candidate_vars = self._results_table.query(query)

        if len(candidate_vars) > 0:

            # Use bedtools to cluster significant markers in 250kbp chunks
            bed_out = Path('cluster.bed')
            bed_in = Path('clustered.bed')
            header = [self._chrom_column, self._pos_column, self._pos_column, self._id_column]
            candidate_vars[header].to_csv(bed_out, sep='\t', index=False, header=False, float_format='%0.0f')

            bed_cmd = f'bedtools cluster -i /test/{bed_out} -d {self._clumping_distance} > {bed_in}'
            self._cmd_executor.run_cmd_on_docker(bed_cmd)

            clustered = pd.read_csv(bed_in, names=['CHROM', 'GENPOS', 'GENPOS2', self._id_column, 'CLUSTER'], sep='\t')
            candidate_vars = candidate_vars.merge(clustered[[self._id_column, 'CLUSTER']], on=self._id_column)

            # And group by cluster to generate our final list of index variants
            index_vars = candidate_vars.loc[candidate_vars.groupby('CLUSTER')[self._p_column].idxmin()]

            self._logger.info(f'{len(index_vars)} index variants at P < {self._sig_threshold:0.3g} found in provided '
                              f'results file.')

            # Cleanup
            bed_in.unlink()
            bed_out.unlink()

        else:
            # Make sure we return an empty DataFrame with the same columns...
            blank_columns = self._results_table.columns.to_list()
            blank_columns.append('CLUSTER')
            index_vars = pd.DataFrame(columns=blank_columns)

            self._logger.info(f'No index variants at P < {self._sig_threshold:0.3g} found in provided '
                              f'results file.')

        return index_vars