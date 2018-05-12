#!/bin/sh
/home/taylordaly31/spark_dna/bowtie2-2.3.4.1/bowtie2 -p 4 --no-hd --no-sq -x $1 -S hdfs:///output/output_$2.sam