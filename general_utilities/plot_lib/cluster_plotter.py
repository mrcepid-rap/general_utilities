import random
from abc import abstractmethod
from pathlib import Path
from typing import Tuple, List, Dict

import pandas as pd

from general_utilities.association_resources import get_include_sample_ids
from general_utilities.import_utils.import_lib import BGENInformation
from general_utilities.job_management.command_executor import CommandExecutor
from general_utilities.plot_lib.plotter import Plotter


class ClusterPlotter(Plotter):

    def __init__(self, cmd_executor: CommandExecutor, results_table: pd.DataFrame, genetic_data: Dict[str, Dict[str, Path]],
                 chrom_column: str, pos_column: str, alt_column: str, id_column: str, p_column: str, csq_column: str,
                 maf_column: str, gene_symbol_column: str, test_name: str = None, sig_threshold: float = 1E-6,
                 clumping_distance=250000):

        super().__init__(cmd_executor)

        self._results_table = results_table
        self._genetic_data = genetic_data

        # Query variables
        self._test_name = test_name
        self._sig_threshold = sig_threshold
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
        self._full_samples, self._subset_samples = self._define_samples()
        self._index_variants = self._cluster_variants()

    def get_index_variant_table(self) -> pd.DataFrame:
        return self._index_variants

    def _define_samples(self) -> Tuple[List[str], List[str]]:

        # Get the full list of samples
        full_samples = get_include_sample_ids()

        # No need for more than ~10k samples to do ld calculation with.
        if len(full_samples) < 1E4:
            subset_samples = full_samples.copy()
        else:
            subset_samples = random.sample(full_samples, 10000)

        return full_samples, subset_samples

    def _cluster_variants(self) -> pd.DataFrame:

        if self._test_name is None:
            query = f'{self._p_column} < {self._sig_threshold}'
        else:
            query = f'TEST == "{self._test_name}" & {self._p_column} < {self._sig_threshold}'

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

    @abstractmethod
    def plot(self) -> Tuple[List[Path], List[Path]]:
        pass
