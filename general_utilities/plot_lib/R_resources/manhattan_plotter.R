#!/usr/bin/env Rscript --vanilla

# WARNING: This script is a poorly optimised time-bomb that could explode at any moment!

library(data.table)
library(tidyverse)
library(patchwork)
library(lemon)

theme <- theme(panel.background=element_rect(fill="white"),line=element_line(size=1,colour="black",lineend="round"),axis.line=element_line(size=1),
               text=element_text(size=16,face="bold",colour="black"),axis.text=element_text(colour="black"),axis.ticks=element_line(size=1,colour="black"),
               axis.ticks.length=unit(.1,"cm"),strip.background=element_rect(fill="white"),axis.text.x=element_text(angle=45,hjust=1),
               legend.position="blank",panel.grid.major=element_line(colour="grey",size=0.5),legend.key=element_blank())
theme.legend <- theme + theme(legend.position="right")