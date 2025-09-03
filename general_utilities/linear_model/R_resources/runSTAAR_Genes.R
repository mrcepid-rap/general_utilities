#!/usr/bin/Rscript --vanilla

# Load required libraries
library(Matrix)
library(STAAR)
library(data.table)
library(jsonlite)

# Read in inputs. They are:
# 1. [1] A sparse matrix[i][j] where i = samples and j = variants
# 2. [2] Variants file that has the same length as j with variant IDs, positions, and MAF
# 3. [3] Samples file that has the same length as i with sample IDs
# 4. [4] The null model file from runSTAAR_Null.R
# 5. [5] Gene name for subsetting the variants file
# 6. [6] Output file path
args <- commandArgs(trailingOnly = T)
matrix_file <-args[1]
variants_file <-args[2]
samples_file <- args[3]
null_model_file <- args[4]
gene <- args[5]
output_file <- args[6]
#
# matrix_file <- '/private/var/folders/z7/4hkdzr016yx6sxqg_bdynqdw0000gp/T/pytest-of-eugene.gardner/pytest-28/test_build_staar_gene_HC_PTV_M0/ENST00000235345.phenotype.STAAR.mtx'
# variants_file <- '/private/var/folders/z7/4hkdzr016yx6sxqg_bdynqdw0000gp/T/pytest-of-eugene.gardner/pytest-28/test_build_staar_gene_HC_PTV_M0/HC_PTV-MAF_001.chr1_chunk3.STAAR.variants_table.tsv'
# samples_file <- '/private/var/folders/z7/4hkdzr016yx6sxqg_bdynqdw0000gp/T/pytest-of-eugene.gardner/pytest-28/test_build_staar_gene_HC_PTV_M0/HC_PTV-MAF_001.chr1_chunk3.STAAR.samples_table.tsv'
# null_model_file <-  '/private/var/folders/z7/4hkdzr016yx6sxqg_bdynqdw0000gp/T/pytest-of-eugene.gardner/pytest-28/test_build_staar_gene_HC_PTV_M0/phenotype.STAAR_null.rds'
# gene <- 'ENST00000235345'
# output_file <- '/private/var/folders/z7/4hkdzr016yx6sxqg_bdynqdw0000gp/T/pytest-of-eugene.gardner/pytest-28/test_build_staar_gene_HC_PTV_M0/ENST00000235345.phenotype.STAAR_results.json'

# Load RDS genotype matrix
genotypes <- readMM(matrix_file)
variants <- fread(variants_file, sep='\t')
variants <- variants[ENST == gene]
samples <- fread(samples_file, sep='\t')

# Assign appropriate column and row names to the genotype matrix
rownames(genotypes) <- as.character(samples[,sampID])
colnames(genotypes) <- as.character(variants[,varID])

# Load the null model file:
obj_nullmodel <- readRDS(null_model_file)

# Trim the genotypes/sparse kinship mtx down to individuals included in the null model file:
poss <- obj_nullmodel$id_include # this gets possible individuals from the null model
genotypes <- genotypes[poss, ,drop=F] # And then use that list to pare down the genotype matrix, drop required to not convert 1d matricies to vectors

# I don't exclude variants that don't exist in the subset of individuals with a given phenotype
# So we have to check here how many variants we actually have for genotypes
tot_vars <- 0
for (i in seq_len(ncol(genotypes))) {
  if (sum(genotypes[,i]) > 0) {
    tot_vars <- tot_vars + 1
  }
}
cMAC <- sum(genotypes)

# Only run STAAR if there is greater than one non-ref variant and there are cases
if (obj_nullmodel$zero_cases == TRUE | tot_vars <= 2) {
  # Else just return NaN to indicate the test was not run
  gene.results <- list(n_model=nrow(genotypes), relatedness_correction=obj_nullmodel$corrected_for_relateds,
                       p_val_O=NaN, p_val_SKAT=NaN,
                       p_val_burden=NaN, p_val_ACAT=NaN,
                       n_var=tot_vars, cMAC=cMAC, model_run=FALSE)

} else {
  staar_result <- STAAR(genotype = genotypes, obj_nullmodel = obj_nullmodel, rare_maf_cutoff = 1)
  gene.results <- list(n_model=nrow(genotypes), relatedness_correction=obj_nullmodel$corrected_for_relateds,
                       p_val_O=staar_result$results_STAAR_O, p_val_SKAT=staar_result$results_STAAR_S_1_25[[1]],
                       p_val_burden=staar_result$results_STAAR_B_1_1[[1]], p_val_ACAT=staar_result$results_STAAR_A_1_25[[1]],
                       n_var=tot_vars, cMAC=cMAC, model_run=TRUE)
}

# And write the final output table
export_JSON <- toJSON(gene.results, flatten = T, digits = NA, auto_unbox = T)
write(export_JSON, output_file)