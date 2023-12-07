import os
import csv
import dxpy
import pandas as pd

from typing import List
from pathlib import Path

from general_utilities.association_resources import build_transcript_table, define_field_names_from_pandas, \
    bgzip_and_tabix
from general_utilities.job_management.command_executor import build_default_command_executor, CommandExecutor
from general_utilities.plot_lib.manhattan_plotter import ManhattanPlotter


def process_linear_model_outputs(output_prefix: str, cmd_executor: CommandExecutor,
                                 is_snp_tar: bool = False, is_gene_tar: bool = False,
                                 gene_infos: list = None) -> List[Path]:

    if gene_infos is not None:
        valid_genes = []
        for gene_info in gene_infos:
            valid_genes.append(gene_info.name)
    else:
        valid_genes = None

    if is_snp_tar:
        os.rename(output_prefix + '.lm_stats.tmp',
                  output_prefix + '.SNP.glm.stats.tsv')
        outputs = [Path(f'{output_prefix}.SNP.glm.stats.tsv')]
    elif is_gene_tar:
        os.rename(output_prefix + '.lm_stats.tmp',
                  output_prefix + '.GENE.glm.stats.tsv')
        outputs = [Path(f'{output_prefix}.GENE.glm.stats.tsv')]
    else:
        # read in the GLM stats file:
        glm_table = pd.read_csv(open(output_prefix + ".lm_stats.tmp", 'r'), sep="\t")

        # Now process the gene table into a useable format:
        # First read in the transcripts file
        transcripts_table = build_transcript_table()

        # Limit to genes we care about if running only a subset:
        if valid_genes is not None:
            transcripts_table = transcripts_table.loc[valid_genes]

        # Add a frequency column
        glm_table['maf'] = glm_table['n_car'] / glm_table['n_model'].multiply(2)

        # Test what columns we have in the 'SNP' field so we can name them...
        field_one = glm_table.iloc[0]
        field_one = field_one['maskname'].split("-")
        field_names = []
        if len(field_one) == 2:  # This could be the standard naming format... check that column [1] is MAF/AC
            if 'MAF' in field_one[1] or 'AC' in field_one[1]:
                field_names.extend(['MASK', 'MAF'])
            else:  # This means we didn't hit on MAF/AC in column [2] and a different naming convention is used...
                field_names.extend(['var1', 'var2'])
        else:
            for i in range(2, len(field_one) + 1):
                field_names.append('var%i' % i)

        # Process the 'SNP' column into separate fields and remove
        glm_table[field_names] = glm_table['maskname'].str.split("-", expand=True)
        glm_table = glm_table.drop(columns=['maskname'])

        # Now merge the transcripts table into the gene table to add annotation and write
        glm_table = pd.merge(transcripts_table, glm_table, on='ENST', how="left")
        gene_tsv = Path(f'{output_prefix}.genes.glm.stats.tsv')
        with gene_tsv.open('w') as gene_out:

            # Sort just in case
            glm_table = glm_table.sort_values(by=['chrom', 'start', 'end'])

            glm_table.to_csv(path_or_buf=gene_out, index=False, sep="\t", na_rep='NA')

        plot_dir = Path(f'{output_prefix}_plots/')  # Path to store plots
        plot_dir.mkdir()

        # Generate manhattan plots:
        for mask in glm_table['MASK'].value_counts().index:

            for maf in glm_table['MAF'].value_counts().index:
                # To note on the below: I use SYMBOL for the id_column parameter below because ENST is the
                # index and I don't currently have a way to pass the index through to the Plotter methods...
                manhattan_plotter = ManhattanPlotter(cmd_executor,
                                                     glm_table.query(f'MASK == "{mask}" & MAF == "{maf}"'),
                                                     chrom_column='chrom', pos_column='start',
                                                     alt_column=None,
                                                     id_column='SYMBOL', p_column='p_val_init',
                                                     csq_column='MASK',
                                                     maf_column='maf', gene_symbol_column='SYMBOL',
                                                     clumping_distance=1,
                                                     maf_cutoff=30 / (glm_table['n_model'].max() * 2),
                                                     sig_threshold=1E-6)

                manhattan_plotter.plot()[0].rename(plot_dir / f'{mask}.{maf}.genes.GLM.stats.png')

        # And bgzip and tabix...
        outputs = list(bgzip_and_tabix(gene_tsv, skip_row=1, sequence_row=2, begin_row=3, end_row=4))
        outputs.append(plot_dir)

    return outputs


# Helper method to just write STAAR outputs from the 'process_staar_outputs' function below
def write_staar_csv(file_path: Path, completed_staar_files: List[str]) -> Path:
    with file_path.open('w') as staar_output:
        output_csv = csv.DictWriter(staar_output,
                                    delimiter="\t",
                                    fieldnames=['SNP', 'n.samps', 'pheno_name', 'relatedness.correction',
                                                'staar.O.p', 'staar.SKAT.p', 'staar.burden.p',
                                                'staar.ACAT.p', 'n_var', 'cMAC'])
        output_csv.writeheader()
        for file in completed_staar_files:
            with open(file, 'r') as curr_file_reader:
                curr_file_csv = csv.DictReader(curr_file_reader, delimiter="\t")
                for gene in curr_file_csv:
                    output_csv.writerow(gene)
            curr_file_reader.close()
        staar_output.close()

    return file_path


# Process STAAR output files
def process_staar_outputs(completed_staar_files: List[str], output_prefix: str, is_snp_tar: bool = False,
                          is_gene_tar: bool = False, gene_infos: list = None) -> List[Path]:

    if is_gene_tar or is_snp_tar:
        if is_gene_tar:
            prefix = 'GENE'
        elif is_snp_tar:
            prefix = 'SNP'
        else:
            raise dxpy.AppError('Somehow output is neither GENE or SNP from STAAR!')

        outputs = [write_staar_csv(Path(f'{output_prefix}.{prefix}.STAAR.stats.tsv'), completed_staar_files)]

    else:
        # Here we are concatenating a temp file of each tsv from completed_staar_files:
        temp_path = write_staar_csv(Path(f'{output_prefix}.genes.STAAR.stats.temp'), completed_staar_files)

        # Now read in the concatenated STAAR stats file:
        staar_table = pd.read_csv(temp_path, sep="\t")

        # Now process the gene table into a useable format:
        # First read in the transcripts file
        transcripts_table = build_transcript_table()

        # Limit to genes we care about if running only a subset:
        if gene_infos is not None:
            valid_genes = []
            for gene_info in gene_infos:
                valid_genes.append(gene_info.name)
            transcripts_table = transcripts_table.loc[valid_genes]

        # Test what columns we have in the 'SNP' field so we can name them...
        field_names = define_field_names_from_pandas(staar_table.iloc[0])
        staar_table[field_names] = staar_table['SNP'].str.split("-", expand=True)
        staar_table = staar_table.drop(columns=['SNP'])  # And drop the SNP column now that we have processed it

        # Now merge the transcripts table into the gene table to add annotation and the write
        staar_table = pd.merge(transcripts_table, staar_table, on='ENST', how="left")
        staar_tsv = Path(f'{output_prefix}.genes.STAAR.stats.tsv')
        with staar_tsv.open('w') as gene_out:

            # Sort just in case
            staar_table = staar_table.sort_values(by=['chrom', 'start', 'end'])
            staar_table.to_csv(path_or_buf=gene_out, index=False, sep="\t", na_rep='NA')

            # And bgzip and tabix...
        outputs = list(bgzip_and_tabix(staar_tsv, skip_row=1, sequence_row=2, begin_row=3, end_row=4))

    return outputs


def merge_glm_staar_runs(output_prefix: str, is_snp_tar: bool = False, is_gene_tar: bool = False) -> List[Path]:

    if is_gene_tar or is_snp_tar:
        if is_gene_tar:
            prefix = 'GENE'
        elif is_snp_tar:
            prefix = 'SNP'
        else:
            raise dxpy.AppError('There should be no way to see this message (error in "merge_glm_staar_runs()")...')

        glm_table = pd.read_csv(output_prefix + '.' + prefix + '.glm.stats.tsv', sep='\t')
        staar_table = pd.read_csv(output_prefix + '.' + prefix + '.STAAR.stats.tsv', sep='\t')

        # We need to pull the ENST value out of the STAAR table 'SNP' variable while trying to avoid any bugs due to
        # file/pheno names
        field_one = staar_table.iloc[0]
        field_one = field_one['SNP'].split("-")
        field_names = []
        if 'ENST' in field_one[0]:
            field_names.append('ENST')
            for i in range(2, len(field_one) + 1):
                field_names.append('var%i' % i)
        else:  # This means we didn't hit on MAF/AC in column [2] and a different naming convention is used...
            raise dxpy.AppError('ENST value is not in the first column of the STAAR table... Error!')

        staar_table[field_names] = staar_table['SNP'].str.split("-", expand=True)

        # Select STAAR columns we need to merge in/match on
        staar_table = staar_table[['ENST', 'pheno_name', 'n_var', 'relatedness.correction', 'staar.O.p', 'staar.SKAT.p',
                                   'staar.burden.p', 'staar.ACAT.p']]

        final_table = glm_table.merge(right=staar_table, on=['ENST', 'pheno_name'])

        gene_path = Path(f'{output_prefix}.{prefix}.STAAR_glm.stats.tsv')
        with gene_path.open('w') as gene_out:
            final_table.to_csv(path_or_buf=gene_out, index=False, sep="\t", na_rep='NA')

        outputs = [gene_path]

    else:
        glm_table = pd.read_csv(f'{output_prefix}.genes.glm.stats.tsv.gz', sep='\t')
        staar_table = pd.read_csv(f'{output_prefix}.genes.STAAR.stats.tsv.gz', sep='\t')

        # Select STAAR columns we need to merge in/match on
        staar_table = staar_table[['ENST', 'MASK', 'MAF', 'pheno_name', 'n_var', 'relatedness.correction', 'staar.O.p',
                                   'staar.SKAT.p', 'staar.burden.p', 'staar.ACAT.p']]

        final_table = glm_table.merge(right=staar_table, on=['ENST', 'MASK', 'MAF', 'pheno_name'])

        gene_path = Path(f'{output_prefix}.genes.STAAR_glm.stats.tsv')
        with gene_path.open('w') as gene_out:

            # Sort just in case
            final_table = final_table.sort_values(by=['chrom', 'start', 'end'])
            final_table.to_csv(path_or_buf=gene_out, index=False, sep="\t", na_rep='NA')

        outputs = list(bgzip_and_tabix(gene_path, skip_row=1, sequence_row=2, begin_row=3, end_row=4))

    return outputs
