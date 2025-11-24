from pathlib import Path
from typing import Tuple, Dict, Any, Set, TypedDict, List

import numpy as np
import pandas as pd
from bgen import BgenReader
from scipy.sparse import csr_matrix

from general_utilities.mrc_logger import MRCLogger

LOGGER = MRCLogger(__name__).get_logger()


class GeneInformation(TypedDict):
    """TypedDict to hold information about a gene and it's variants."""

    chrom: str
    min: int
    max: int
    vars: Set[str]


def make_variant_list(variant_list: pd.DataFrame) -> Dict[str, GeneInformation]:
    """Extracts a list of variants for all ENSTs in a pandas DataFrame.

    The provided DataFrame MUST have the columns: 'ENST', 'CHROM', 'POS', and 'varID'. This format is the standard
    file that is produced by the CollapseVariants tool, and is provided in output tar files as
    '*.STAAR.variants_table.tsv'.

    :param variant_list: DataFrame containing variants.
    :return: A pandas DataFrameGroupBy object where each group corresponds to a unique ENST, and each ENST has information
        about its chromosome, minimum and maximum positions, and a set of variant IDs.
    """

    # First aggregate across genes to generate a list of genes and their respective variants
    search_list = variant_list.groupby('ENST').aggregate(
        CHROM=('CHROM', 'first'),
        MIN=('POS', 'min'),
        MAX=('POS', 'max'),
        VARS=('varID', set)
    )

    # Format into a dictionary to make the information more accessible
    gene_dict = {}
    for current_gene in search_list.itertuples():
        gene_dict[current_gene.Index] = GeneInformation(
            chrom=current_gene.CHROM,
            min=current_gene.MIN,
            max=current_gene.MAX,
            vars=current_gene.VARS
        )

    return gene_dict


def generate_csr_matrix_from_bgen(bgen_path: Path, sample_path: Path, variant_filter_list: Set[str],
                                  chromosome: str = None, start: int = None, end: int = None,
                                  should_collapse_matrix: bool = True,
                                  sample_filter_list: List[str] = None) -> Tuple[csr_matrix, Dict[str, Any]]:
    """
    Convert BGEN genotypes into a sparse matrix format.

    Creates a CSR matrix from BGEN file genotypes for use in STAAR/GLM association testing.
    The matrix represents samples as rows and either genes or variants as columns.

    Note that in order to optimise this process we are collapsing the variants here (stored in the CSR matrix part
    of our output tuple). We are also saving gene and sample level variant information in a dictionary,
    so that this information can be used downstream (for example, when generating the log file).
    There is also a functionality where if should_collapse_matrix is set to False, then we append the entire variant
    matrix in a CSR format. This is used in the associationtesting modules downstream. The rationale for doing it this
    way is that we can save memory when collapsing, but still be able to call the un-collapsed matrices when needed.

    :param bgen_path: Path to the BGEN file.
    :param sample_path: Path to the sample file.
    :param variant_filter_list: A set of variant IDs to extract from the BGEN file.
    :param chromosome: Chromosome to filter variants by (optional).
    :param start: Start position to filter variants by (optional).
    :param end: End position to filter variants by (optional).
    :param should_collapse_matrix: If True (default) collapse variants - sum variants per gene.
        If False, don't sum variants per gene and return the matrix instead.
    :param sample_filter_list: Optional list of sample IDs to include. If None, all samples are used.
    :return: A tuple containing:
        - csr_matrix: A sparse matrix with shape (n_samples, n_genes_or_variants).
        - summary_dict: A dictionary with summary information for each gene.
    """

    with BgenReader(bgen_path, sample_path=sample_path, delay_parsing=True) as bgen_reader:

        # --- Detect and align chromosome naming convention ---
        first_variant = next(iter(bgen_reader))
        bgen_chrom = first_variant.chrom

        if isinstance(chromosome, int):
            chromosome = str(chromosome)
        elif bgen_chrom.lower().startswith("chr") and not chromosome.lower().startswith("chr"):
            chromosome = "chr" + str(chromosome)
        elif not bgen_chrom.lower().startswith("chr") and chromosome.lower().startswith("chr"):
            chromosome = str(chromosome)[3:]

        # Re‑open BGEN after peeking at the first variant
        bgen_reader = BgenReader(bgen_path, sample_path=sample_path, delay_parsing=True)

        # Create sample mask if sample_filter_list is provided
        if sample_filter_list is not None:
            print(f"DEBUG: sample_filter_list provided with {len(sample_filter_list)} samples")
            all_samples = bgen_reader.samples
            print(f"DEBUG: BGEN has {len(all_samples)} total samples")
            # Create a boolean mask for samples to keep
            sample_mask = np.array([s in sample_filter_list for s in all_samples])
            n_samples = np.sum(sample_mask)
            print(f"DEBUG: After filtering, keeping {n_samples} samples")

            if n_samples == 0:
                raise ValueError("No samples from sample_filter_list found in BGEN file")
        else:
            print("DEBUG: No sample_filter_list provided, using all samples")
            sample_mask = None
            n_samples = len(bgen_reader.samples)

        # Fetch actual data from the BGEN file
        # Handle chromosome, start, and end filtering
        if chromosome is not None and start is None and end is None:
            # chromosome is provided, but not start or end
            variants = bgen_reader.fetch(chromosome)  # Fetch all variants on the specified chromosome

        elif [chromosome, start, end].count(None) == 2:
            # Unclear what the user wants if they have provided two of the three
            raise ValueError("If start or end is provided, chromosome must also be provided.")

        else:
            variants = bgen_reader.fetch(chromosome, start, end)

        # create a store for the variant level information
        variant_arrays = []
        variant_n = 0

        # collect genotype arrays for each variant
        for current_variant in variants:

            if variant_filter_list is not None and current_variant.rsid not in variant_filter_list:
                # if we have a variant filter list, skip variants that are not in the filter list Don't ask me why
                # the not / not logic works here, but it does so don't change it I think it's because python parses
                # the variant_filter_list 'not' first and if it fails it doesn't continue to the second 'not'
                # statement, which would otherwise raise an error when using 'in' for NoneType.
                continue

            variant_n += 1  # Have to increment variant_n here, as enumerate would capture skipped variants

            # pull out the actual genotypes
            current_probabilities = current_variant.probabilities

            # store variant codings
            variant_array = np.where(current_probabilities[:, 1] == 1, 1,
                                     np.where(current_probabilities[:, 2] == 1, 2, 0))

            # Apply sample filter if provided
            if sample_mask is not None:
                variant_array = variant_array[sample_mask]

            # store variant level information in the array we created
            variant_arrays.append(variant_array)

        # stack the variant information for all variants in the gene
        stacked_variants = np.column_stack(variant_arrays)

        # if we are collapsing here (to save on memory), leave as False. If set to True, we won't collapse
        # and instead the uncollapsed stacked variants will be appended
        # note the vector naming convention in this small section is a bit hacky, but we want the vectors naming
        # to be consistent so it works for the rest of the function
        if should_collapse_matrix is True:
            stacked_variants = stacked_variants.sum(axis=1)
            variant_n = 1
            stacked_variants = np.reshape(stacked_variants, (-1, 1))  # Reshape to ensure it is a 2D array

        # record the collapsing stats in a dict
        summary_dict = {
            'allele_count': np.sum(stacked_variants),  # use np.sum to get total of all values
            'n_variants': len(variant_arrays),  # get number of variants, based on pre-collapse number of variants!
            'n_columns': variant_n
        }

        # convert this to a csr matrix
        final_genotypes = csr_matrix(stacked_variants, shape=(n_samples, variant_n))

    return final_genotypes, summary_dict