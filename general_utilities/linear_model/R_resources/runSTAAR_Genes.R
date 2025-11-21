#!/usr/bin/Rscript --vanilla

library(Matrix)
library(STAAR)
library(data.table)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)
matrix_file <- args[1]
variants_file <- args[2]
samples_file <- args[3]
null_model_file <- args[4]
gene <- args[5]
output_file <- args[6]

# Load data
genotypes <- readMM(matrix_file)
variants <- fread(variants_file, sep="\t")
variants <- variants[ENST == gene]
samples <- fread(samples_file, sep="\t")

rownames(genotypes) <- as.character(samples$sampID)
colnames(genotypes) <- as.character(variants$varID)

obj_nullmodel <- readRDS(null_model_file)

cat("\n================ DEBUG: ALIGNMENT CHECK ================\n")
cat(sprintf("Genotype matrix: %d x %d\n", nrow(genotypes), ncol(genotypes)))
cat(sprintf("Variants: %d\n", nrow(variants)))
cat(sprintf("Samples: %d\n\n", nrow(samples)))

# NULL model sample IDs
null_ids <- as.character(obj_nullmodel$sample.id)

cat("First 5 genotype sample IDs:\n")
print(head(rownames(genotypes), 5))

cat("First 5 NULL model sample IDs:\n")
print(head(null_ids, 5))

# Overlap
overlap_ids <- intersect(null_ids, rownames(genotypes))
cat(sprintf("\nOverlapping sample IDs: %d\n", length(overlap_ids)))

if (length(overlap_ids) == 0) {
    stop("ERROR: No overlapping samples between genotypes and NULL model!")
}

# Reorder genotypes to match NULL model order
ordered_ids <- null_ids[null_ids %in% overlap_ids]

genotypes <- genotypes[match(ordered_ids, rownames(genotypes)), , drop=FALSE]

cat("\n================ DEBUG: AFTER SUBSETTING ================\n")
cat(sprintf("Genotype matrix now: %d x %d\n", nrow(genotypes), ncol(genotypes)))
cat("First 5 aligned rownames:\n")
print(head(rownames(genotypes), 5))

# Rewrite NULL model internal ordering to match trimmed genotype matrix
obj_nullmodel$sample.id <- ordered_ids
obj_nullmodel$id_include <- seq_len(nrow(genotypes))

# DO NOT touch Sigma_i — it uses sample.id automatically inside STAAR

cat("\n================ DEBUG: READY FOR STAAR ================\n")
cat(sprintf("Final genotype dim: %d x %d\n", nrow(genotypes), ncol(genotypes)))
cat(sprintf("Final NULL model sample.id: %d\n", length(obj_nullmodel$sample.id)))
cat("First 5 final sample IDs:\n")
print(head(obj_nullmodel$sample.id, 5))

# Count variants
tot_vars <- sum(colSums(genotypes) > 0)
cMAC <- sum(genotypes)

# Run STAAR
if (obj_nullmodel$zero_cases == TRUE | cMAC <= 2) {
    gene.results <- list(
        n_model = nrow(genotypes),
        relatedness_correction = obj_nullmodel$corrected_for_relateds,
        p_val_O = NaN, p_val_SKAT = NaN,
        p_val_burden = NaN, p_val_ACAT = NaN,
        n_var = tot_vars, cMAC = cMAC,
        model_run = FALSE
    )
} else {
    staar_result <- STAAR(genotype = genotypes, obj_nullmodel = obj_nullmodel, rare_maf_cutoff = 1)
    gene.results <- list(
        n_model=nrow(genotypes),
        relatedness_correction=obj_nullmodel$corrected_for_relateds,
        p_val_O=staar_result$results_STAAR_O,
        p_val_SKAT=staar_result$results_STAAR_S_1_25[[1]],
        p_val_burden=staar_result$results_STAAR_B_1_1[[1]],
        p_val_ACAT=staar_result$results_STAAR_A_1_25[[1]],
        n_var=tot_vars, cMAC=cMAC,
        model_run=TRUE
    )
}

write(toJSON(gene.results, flatten=TRUE, digits=NA, auto_unbox=TRUE), output_file)