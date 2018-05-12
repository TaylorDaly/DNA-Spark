from pyspark import SparkConf, SparkContext, TaskContext, SparkFiles
import math
import os
import time
from pyspark.sql import SQLContext


conf = SparkConf().setAppName("DNA") # .setMaster("local") # .setAppName("DNA")

sc = SparkContext(conf=conf)
input_file = "hdfs:///spark_dna/SRR043378_1.fastq"
# input_file = "hdfs:///spark_dna/yeast_40K_reads.fq"

# creates tuples with format (string, line number)
raw_input = (sc.textFile(input_file)).zipWithIndex()

# create tuple with format (read number, string)
indexed_raw_input = raw_input.map(lambda x: (math.floor(x[1]/4), x[0]))


# Function to pass to map function that concatenates the 4 strings
def make_reads(iterable_read):
    x = 0
    line1 = ""
    line2 = ""
    line3 = ""
    line4 = ""
    for line in iterable_read:
        if x == 0:
            line1 = line
        if x == 1:
            line2 = line
        if x == 2:
            line3 = line
        if x == 3:
            line4 = line
        x = x+1
    return "%s\n%s\n%s\n%s\n" % (line1, line2, line3, line4)


reads_tuple = indexed_raw_input.groupByKey().mapValues(lambda x: make_reads(x))

# test = reads_tuple.take(10)
# print(test[0][1])

# Sort by key, which is just the line number, and then get the values, which is just the read.
readsRDD = reads_tuple.sortByKey().values()


# Test Bowtie2 index for smaller yeast DNA
# bowtie_index = "/home/taylordaly31/Bowtie2Index/genome"

# Absolute path to bowtie index on each worker
bowtie_index = "/home/taylordaly31/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.bowtie_index"

# bowtie_script = "/home/taylordaly31/spark_dna/bowtie_run_script.sh"
# bowtie_script_name = "bowtie_run_script.sh"
# sc.addFile(bowtie_script)

# Run script that starts bowtie with parameters to bowtie index.
alignment_pipe = readsRDD.pipe("/home/taylordaly31/bowtie2-2.3.4.1/bowtie2 -p 4 --no-hd --no-sq -x " + bowtie_index + " -")


def reduce_to_sam(output):
    try:
        file = open("output.sam", "a+")
        for alignment in output:
            file.write(alignment + '\n')
        file.close()
    except Exception as ex:
        print(ex)


start = time.time()
aligned_output = alignment_pipe.foreachPartition(lambda partition: reduce_to_sam(partition))
end = time.time()
print("Runtime: " + str(end-start))

sc.stop()

