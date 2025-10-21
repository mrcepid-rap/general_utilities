#!/usr/bin/env Rscript --vanilla

library(data.table)
library(ggplot2)
library(lemon)
library(patchwork)
library(stringr)

# Setup basic themes for ggplot:
theme <- theme(panel.background=element_rect(fill="white"),line=element_line(linewidth=1,colour="black",lineend="round"),axis.line=element_line(linewidth=1),
               text=element_text(size=16,face="bold",colour="black"),axis.text=element_text(colour="black"),axis.ticks=element_line(linewidth=1,colour="black"),
               axis.ticks.length=unit(.1,"cm"),strip.background=element_rect(fill="white"),axis.text.x=element_text(angle=45,hjust=1),
               legend.position="blank",panel.grid.major=element_line(colour="grey",linewidth=0.5),legend.key=element_blank())
theme.legend <- theme + theme(legend.position="right")

standardize_table <- function(table_path, p_val_col) {

  some_table <- fread(table_path)

  p_val_col <- str_to_lower(p_val_col)

  setnames(some_table, names(some_table), str_to_lower(names(some_table)))
  setnames(some_table, p_val_col, "p_value_selected")

  some_table$p_value_selected <- as.numeric(some_table$p_value_selected)

  some_table[,log_p:=-log10(p_value_selected)] # Add -log10 p value
  if ('chr' %in% names(some_table)) {  # Account for BOLT-LMM results...
    setnames(some_table, 'chr', 'chrom')
    some_table[,chrom:=as.character(chrom)]
  }
  some_table[,chrom:=factor(chrom, levels = mean_chr_pos[,chrom])] # Make sure chromosomes are standardised to those in transcripts

  return(some_table)

}

load_and_plot_data <- function(result_path, label_path, p_val_col, plot_header, p_sig, p_sugg, label_qq) {

  # Read in and standardize tables
  result_table <- standardize_table(result_path, p_val_col)
  label_table <- standardize_table(label_path, p_val_col)

  # Get the y max value empirically from the data:
  # Also set to a minimum of 10 so we don't get squashed plots
  max_p <- result_table[!is.infinite(log_p),max(log_p,na.rm = T)]
  if (max_p > 10) {
    ymax <- max_p + (max_p * 0.05)
  } else {
    ymax <- 10
  }

  manh.plot <- plot_manh(stats = result_table[!is.infinite(log_p)], ymax=ymax, p_sig = p_sig, p_sugg = p_sugg,
                         label_data = label_table)
  qq.plot <- plot_qq(result_table[!is.infinite(log_p)], ymax=ymax, label.markers = label_qq)

  # Decide header
  if (plot_header != 'None') {
    annotation <- plot_annotation(title = plot_header, theme = theme(plot.title=element_text(size=18,face="bold",colour="black")))
  } else {
    annotation <- plot_annotation(theme = theme(plot.title=element_text(size=18,face="bold",colour="black")))
  }

  comb.plot <- manh.plot + qq.plot +
    plot_layout(ncol = 2, nrow = 1, widths = c(3,1.3)) + annotation


  comb.plot <- comb.plot


  return(comb.plot)

}

plot_manh <- function(stats, ymax, p_sig, p_sugg, label_data) {

  if (!is.na(p_sugg)) {
    sig.lines <- -log10(c(p_sig,p_sugg))
    sig.colours <- c('red','orange')
  } else {
    sig.lines <- -log10(c(p_sig))
    sig.colours <- c('red')
  }

  manh.plot <- ggplot(stats, aes(manh.pos, log_p, colour = chrom)) +
    geom_point(size=0.25) +
    geom_hline(yintercept = sig.lines, colour = sig.colours, linetype = 2) +
    scale_x_continuous(name = "Chromosome", label = mean_chr_pos[,chrom], breaks = mean_chr_pos[,manh.pos]) +
    scale_y_continuous(name = expression(bold(-log[10](italic(p)))), limits = c(0,ymax)) +
    scale_colour_manual(values = rep(c("#93328E","#0047BB"),12)[1:23], breaks = c(as.character(1:22),"X")) +
    coord_capped_cart(bottom="both",left="both") + # Comes from the "lemon" package
    theme + theme(panel.grid.major = element_blank())

  if (nrow(label_data) != 0) {
    manh.plot <- manh.plot + geom_text(inherit.aes = F, data = label_data, aes(manh.pos, log_p, label = symbol),hjust=0,angle=45, size=3)
  }

  return(manh.plot)

}

plot_qq <- function(stats, ymax, label.y = FALSE, is.null = FALSE, label.markers = TRUE) {

  ## QQplot
  qqplot.data <- data.table(observed = stats[,p_value_selected],
                            symbol = stats[,symbol])
  setkey(qqplot.data,observed)
  qqplot.data <- qqplot.data[!is.na(observed)]
  lam <- qqplot.data[,median(qchisq(1 - observed, 1))] / qchisq(0.5, 1)
  qqplot.data[,observed:=-log10(observed)]
  qqplot.data[,expected:=-log10(ppoints(nrow(qqplot.data)))]

  # make sure we set a reasonable y-max based on number of points (e.g. GWAS/Burden
  # will be different)
  expected_ymax <- qqplot.data[,ceiling(max(expected))]

  if (is.null) {

    qq.plot <- ggplot() +
      annotate(geom='text', x=expected_ymax / 2, y = ymax / 2, label = 'NULL QQ PLOT', hjust = 0.5, vjust = 0.5,size=10)

  } else {

    qq.plot <- ggplot(qqplot.data, aes(expected, observed)) +
      geom_abline(slope = 1, intercept = 0,colour="red") +
      geom_point(size=0.25)

    if (label.markers) {
      qq.plot <- qq.plot +
        geom_text(inherit.aes = F, data = qqplot.data[observed > -log10(1.4e-6)], aes(expected, observed, label = symbol), position = position_nudge(-0.04,0),hjust=1, size=3)
    }
  }

  qq.plot <- qq.plot +
    scale_x_continuous(name = expression(bold(Expected~-log[10](italic(p)))), limits = c(0,expected_ymax)) +
    scale_y_continuous(name = ifelse(label.y,expression(bold(Observed~-log[10](italic(p)))),expression('')), limits = c(0,ymax)) +
    annotate('text', x = 1, y = expected_ymax * 0.9, hjust=0, vjust=0.5, label = as.expression(bquote(lambda == .(lam)))) +
    theme + theme(panel.grid.major = element_blank())

  if (!label.y) {
    qq.plot <- qq.plot + theme(axis.ticks.y = element_blank(), axis.text.y = element_blank())
  }

  return(qq.plot)

}

# Inputs are:
# 1: summary_table
# 2: label data
# 3: p_val column
# 4: plot name
# 5: p.sig
# 6: p.sugg
# 7: label qq plots
args <- commandArgs(trailingOnly = T)
mean_chr_pos <- fread('/test/mean_chr_pos.tsv')

manh_plot <- load_and_plot_data(args[1], args[2], args[3], args[4], as.numeric(args[5]), as.numeric(args[6]), as.logical(args[7]))

ggsave('/test/manhattan_plot.png', manh_plot, units='in', width = 15, height = 6)