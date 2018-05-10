from pyspark import SparkConf, SparkContext, TaskContext
import math
import os
from pyspark.sql import SQLContext


conf = SparkConf().setMaster("local").setAppName("DNA")

sc = SparkContext(conf=conf)
# input_file = "SRR043378_1.fastq/data"
input_file = "yeast_40K_reads.fq"

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

# Sort by key, which is just the line number, and then get the values, which is just the read.
readsRDD = reads_tuple.sortByKey().values()

# bowtie_index = "GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.bowtie_index/" \
#                "GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.bowtie_index"
bowtie_index = "Bowtie2Index/genome"

alignment_pipe = readsRDD.pipe("bowtie2 --no-hd --no-sq -x " + bowtie_index + " -")


def reduce_to_sam(output):
    try:
        if not os.path.exists("output"):
            os.makedirs("output")
        ctx = TaskContext()
        _id = str(ctx.partitionId())
        file = open("output/output_"+_id+".sam", "a+")
        file.write(output+'\n')
        file.close()
    except Exception as ex:
        print(ex)


alignment_pipe.foreach(lambda x: reduce_to_sam(x))

sc.stop()

