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
args <- commandArgs(trailingOnly = TRUE)
matrix_file     <- args[1]
variants_file   <- args[2]
samples_file    <- args[3]
null_model_file <- args[4]
gene            <- args[5]
output_file     <- args[6]

# Load genotype matrix, variants and samples
genotypes <- readMM(matrix_file)
variants  <- fread(variants_file, sep = "\t")
variants  <- variants[ENST == gene]
samples   <- fread(samples_file, sep = "\t")

# Assign appropriate row and column names
rownames(genotypes) <- as.character(samples[, sampID])
colnames(genotypes) <- as.character(variants[, varID])

# Load the null model file:
obj_nullmodel <- readRDS(null_model_file)

# --- Align genotypes with NULL model + GRM ---

# NULL model sample lists
null_ids <- obj_nullmodel$id_include
grm_ids  <- obj_nullmodel$sample.id   # sparse GRM sample IDs

# 1. Intersect all sample sets
valid_ids <- intersect(null_ids, grm_ids)
valid_ids <- intersect(valid_ids, rownames(genotypes))

if (length(valid_ids) == 0) {
    stop("No overlapping samples between genotypes, NULL model, and GRM")
}

# 2. Sort valid_ids in the EXACT order expected by the NULL model
valid_ids <- null_ids[null_ids %in% valid_ids]

# 3. Reorder genotype matrix to NULL model order
genotypes <- genotypes[match(valid_ids, rownames(genotypes)), , drop = FALSE]

cat(sprintf("Final aligned samples: %d\n", nrow(genotypes)))
cat(sprintf("NULL model (id_include): %d\n", length(null_ids)))
cat(sprintf("GRM (sample.id):         %d\n", length(grm_ids)))

# 4. Rewrite NULL MODEL internals to the same ordering
# Sigma_i == sparse GRM matrix in NULL model
if (!is.null(obj_nullmodel$Sigma_i)) {
    idx <- match(valid_ids, obj_nullmodel$sample.id)
    obj_nullmodel$Sigma_i <- obj_nullmodel$Sigma_i[idx, idx]
}

# Update model sample lists too
obj_nullmodel$id_include <- valid_ids
obj_nullmodel$sample.id  <- valid_ids

# --- Variant counts ---
tot_vars <- 0
for (i in seq_len(ncol(genotypes))) {
    if (sum(genotypes[, i]) > 0) {
        tot_vars <- tot_vars + 1
    }
}
cMAC <- sum(genotypes)

# Only run STAAR if there is greater than one non-ref variant and there are cases
if (obj_nullmodel$zero_cases == TRUE || cMAC <= 2) {
    gene.results <- list(
        n_model               = nrow(genotypes),
        relatedness_correction= obj_nullmodel$corrected_for_relateds,
        p_val_O               = NaN,
        p_val_SKAT            = NaN,
        p_val_burden          = NaN,
        p_val_ACAT            = NaN,
        n_var                 = tot_vars,
        cMAC                  = cMAC,
        model_run             = FALSE
    )
} else {
    staar_result <- STAAR(
        genotype     = genotypes,
        obj_nullmodel= obj_nullmodel,
        rare_maf_cutoff = 1
    )
    gene.results <- list(
        n_model               = nrow(genotypes),
        relatedness_correction= obj_nullmodel$corrected_for_relateds,
        p_val_O               = staar_result$results_STAAR_O,
        p_val_SKAT            = staar_result$results_STAAR_S_1_25[[1]],
        p_val_burden          = staar_result$results_STAAR_B_1_1[[1]],
        p_val_ACAT            = staar_result$results_STAAR_A_1_25[[1]],
        n_var                 = tot_vars,
        cMAC                  = cMAC,
        model_run             = TRUE
    )
}

# Write the final output table
export_JSON <- toJSON(gene.results, flatten = TRUE, digits = NA, auto_unbox = TRUE)
write(export_JSON, output_file)