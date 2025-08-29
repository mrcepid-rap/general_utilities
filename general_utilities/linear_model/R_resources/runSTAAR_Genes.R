#!/usr/bin/Rscript --vanilla

# Load required libraries
library(Matrix)
library(STAAR)
library(data.table)

# Read in inputs. They are:
# 1. [1] A sparse matrix[i][j] where i = samples and j = variants
# 2. [2] Variants file that has the same length as j with variant IDs, positions, and MAF
# 3. [3] The null model file from runSTAAR_Null.R
# 4. [4] Pheno name for including in output table
# 5. [5] File output prefix
# 6. [6] Current chromosome being run for output purposes
# 7. [7] If only running a subset of genes, define here. Default is 'none'
args <-commandArgs(trailingOnly = T)
matrix_file <-args[1]
variants_file <-args[2]
samples_file <- args[3]
null_model_file <- args[4]
output_file <- args[5]

# Load RDS genotype matrix
genotypes <- readMM(matrix_file)
variants <- fread(variants_file, sep='\t')
samples <- fread(samples_file, sep='\t')

# Assign appropriate column and row names to the genotype matrix
rownames(genotypes) <- as.character(samples[,sampID])
colnames(genotypes) <- as.character(variants[,varID])

# Load the null model file:
obj_nullmodel <- readRDS(null_model_file)

# Trim the genotypes/sparse kinship mtx down to individuals included in the null model file:
poss <- obj_nullmodel$id_include # this gets possible individuals from the null model
genotypes <- genotypes[poss, .] # And then use that list to pare down the genotype matrix

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
  gene.results <- data.table(n.samps=nrow(genotypes), relatedness.correction=obj_nullmodel$corrected_for_relateds,
                             staar.O.p=NaN, staar.SKAT.p=NaN,
                             staar.burden.p=NaN, staar.ACAT.p=NaN,
                             n_var=tot_vars, cMAC=cMAC)

} else {
  staar_result <- STAAR(genotype = genotypes, obj_nullmodel = obj_nullmodel, rare_maf_cutoff = 1)
  gene.results <- data.table(n.samps=nrow(genotypes), relatedness.correction=obj_nullmodel$corrected_for_relateds,
                             staar.O.p=staar_result$results_STAAR_O, staar.SKAT.p=staar_result$results_STAAR_S_1_25[[1]],
                             staar.burden.p=staar_result$results_STAAR_B_1_1[[1]], staar.ACAT.p=staar_result$results_STAAR_A_1_25[[1]],
                             n_var=tot_vars, cMAC=cMAC)
}

# And write the final output table
# Remove the geneID and mask values as they are redundant at this point:
gene.results[,geneID:=NULL]
gene.results[,mask:=NULL]
fwrite(gene.results, output_file, sep = "\t", quote = F, row.names = F, col.names = T, na = "NaN")