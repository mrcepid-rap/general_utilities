#!/usr/bin/Rscript --vanilla
# Modified runSTAAR_Genes.R with extensive debugging

library(Matrix)
library(STAAR)
library(data.table)
library(jsonlite)

args <- commandArgs(trailingOnly = T)
matrix_file <- args[1]
variants_file <- args[2]
samples_file <- args[3]
null_model_file <- args[4]
gene <- args[5]
output_file <- args[6]

cat("\n", rep("=", 80), "\n", sep="")
cat("STAAR GENE TEST DEBUG:", gene, "\n")
cat(rep("=", 80), "\n\n", sep="")

# Load genotype matrix
cat("Loading matrix from:", matrix_file, "\n")
genotypes <- readMM(matrix_file)
cat("✓ Matrix loaded: ", nrow(genotypes), " samples x ", ncol(genotypes), " variants\n", sep="")
cat("  Non-zero entries:", length(which(genotypes > 0)), "\n")
cat("  Total allele count:", sum(genotypes), "\n")

# Load variants
variants <- fread(variants_file, sep='\t')
variants <- variants[ENST == gene]
cat("✓ Variants loaded:", nrow(variants), "variants for gene", gene, "\n")

# Load samples
samples <- fread(samples_file, sep='\t')
cat("✓ Samples table loaded:", nrow(samples), "samples\n")
cat("  First 5 sample IDs:", head(samples$sampID, 5), "\n")

# Check dimension match
if (nrow(genotypes) != nrow(samples)) {
  cat("⚠️ WARNING: Dimension mismatch!\n")
  cat("  Matrix rows:", nrow(genotypes), "\n")
  cat("  Samples rows:", nrow(samples), "\n")
  cat("  This should NOT happen if Python subsetting worked correctly!\n")
}

# Assign column names
colnames(genotypes) <- as.character(variants[,varID])
cat("✓ Column names assigned\n")

# Load null model
cat("\nLoading null model from:", null_model_file, "\n")
obj_nullmodel <- readRDS(null_model_file)
poss <- obj_nullmodel$id_include
cat("✓ Null model loaded\n")
cat("  Null model includes:", length(poss), "samples\n")
cat("  First 5 null model IDs:", head(poss, 5), "\n")
cat("  Relatedness correction:", obj_nullmodel$corrected_for_relateds, "\n")

# Assign rownames
rownames(genotypes) <- as.character(samples[,sampID])
cat("✓ Row names assigned\n")

# Check overlap before filtering
overlap <- sum(rownames(genotypes) %in% poss)
cat("\nSample overlap check:\n")
cat("  Samples in matrix:", nrow(genotypes), "\n")
cat("  Samples in null model:", length(poss), "\n")
cat("  Overlapping samples:", overlap, "(", round(100*overlap/nrow(genotypes), 1), "%)\n")

if (overlap < nrow(genotypes) * 0.9) {
  cat("⚠️ WARNING: Low overlap! Only", round(100*overlap/nrow(genotypes), 1), "% overlap\n")
  missing <- rownames(genotypes)[!rownames(genotypes) %in% poss]
  cat("  Samples in matrix but not null model:", length(missing), "\n")
  cat("  Example missing:", head(missing, 5), "\n")
}

# Trim genotypes to null model samples
genotypes <- genotypes[rownames(genotypes) %in% poss, , drop=F]
cat("\n✓ Filtered to null model samples\n")
cat("  Matrix after filtering: ", nrow(genotypes), " samples x ", ncol(genotypes), " variants\n", sep="")

# Check variants with data
tot_vars <- 0
for (i in seq_len(ncol(genotypes))) {
  if (sum(genotypes[,i]) > 0) {
    tot_vars <- tot_vars + 1
  }
}
cMAC <- sum(genotypes)

cat("\nVariant summary:\n")
cat("  Total variants:", ncol(genotypes), "\n")
cat("  Variants with data:", tot_vars, "\n")
cat("  cMAC (carrier MAC):", cMAC, "\n")

# Count carriers
carriers <- rowSums(genotypes) > 0
n_carriers <- sum(carriers)
cat("  Number of carriers:", n_carriers, "\n")

if (n_carriers > 0) {
  cat("  Carrier IDs (first 10):", head(rownames(genotypes)[carriers], 10), "\n")
}

# Special debug for GCK
if (gene == "ENST00000403799") {
  cat("\n", rep("!", 80), "\n", sep="")
  cat("!!! SPECIAL DEBUG FOR GCK !!!\n")
  cat(rep("!", 80), "\n\n", sep="")

  cat("Detailed carrier info:\n")
  for (i in 1:min(5, ncol(genotypes))) {
    var_carriers <- which(genotypes[,i] > 0)
    if (length(var_carriers) > 0) {
      cat("  Variant", i, "(", colnames(genotypes)[i], "):",
          length(var_carriers), "carriers\n")
      cat("    Carrier IDs:", head(rownames(genotypes)[var_carriers], 10), "\n")
    }
  }
}

# Run STAAR
cat("\n", rep("-", 80), "\n", sep="")
if (obj_nullmodel$zero_cases == TRUE | cMAC <= 2) {
  cat("⚠️ SKIPPING STAAR TEST: ",
      if(obj_nullmodel$zero_cases) "no cases" else paste("cMAC too low (", cMAC, ")", sep=""),
      "\n")
  gene.results <- list(n_model=nrow(genotypes), relatedness_correction=obj_nullmodel$corrected_for_relateds,
                       p_val_O=NaN, p_val_SKAT=NaN,
                       p_val_burden=NaN, p_val_ACAT=NaN,
                       n_var=tot_vars, cMAC=cMAC, model_run=FALSE)
} else {
  cat("✓ Running STAAR test...\n")
  staar_result <- STAAR(genotype = genotypes, obj_nullmodel = obj_nullmodel, rare_maf_cutoff = 1)
  cat("✓ STAAR completed\n")
  cat("  P-values:\n")
  cat("    Burden:", staar_result$results_STAAR_B_1_1[[1]], "\n")
  cat("    SKAT:", staar_result$results_STAAR_S_1_25[[1]], "\n")
  cat("    O:", staar_result$results_STAAR_O, "\n")
  cat("    ACAT:", staar_result$results_STAAR_A_1_25[[1]], "\n")

  gene.results <- list(n_model=nrow(genotypes), relatedness_correction=obj_nullmodel$corrected_for_relateds,
                       p_val_O=staar_result$results_STAAR_O, p_val_SKAT=staar_result$results_STAAR_S_1_25[[1]],
                       p_val_burden=staar_result$results_STAAR_B_1_1[[1]], p_val_ACAT=staar_result$results_STAAR_A_1_25[[1]],
                       n_var=tot_vars, cMAC=cMAC, model_run=TRUE)
}

cat(rep("=", 80), "\n\n", sep="")

# Write output
export_JSON <- toJSON(gene.results, flatten = T, digits = NA, auto_unbox = T)
write(export_JSON, output_file)