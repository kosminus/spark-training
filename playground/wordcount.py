import time

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("wordcount")

sc = SparkContext(conf=conf)

input_file_path = "../data/input/names.txt"
output_directory = "../data/output/word_count"


lines = sc.textFile(input_file_path, minPartitions=3)

def splitWords(line):
    return line.split(" ")


words = lines.flatMap(lambda line: line.split(" "))
# words = lines.flatMap(splitWords)

word_pairs = words.map(lambda word: (word, 1))
words_counts = word_pairs.reduceByKey(lambda x, y: x + y)


words_counts.saveAsTextFile(output_directory)


time.sleep(500)



