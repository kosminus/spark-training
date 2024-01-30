import time

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("rdds")
sc = SparkContext(conf=conf)


#1. creeaza un RDD dintr-o lista de numere
o_lista_de_numere = list(range(1, 100))
o_lista_de_cuvinte = ["hello", "world", "this", "is", "spark"]


rdd_numere = sc.parallelize(o_lista_de_numere)
rdd_cuvinte = sc.parallelize(o_lista_de_cuvinte)

#read from file

input_file_path = "../data/input/apache_log.txt"

def functie_filtru(line):
    return "404" in line


lista_ipuri = sc.textFile(input_file_path, minPartitions=4) \
    .filter(lambda line: "404" in line) \
    .map(lambda line: line.split(" ")[0]) \
    .distinct()

nr = lista_ipuri.getNumPartitions()
print(f"nr of partitions {nr}")

# for ip in lista_ipuri[:10]:
#     print(ip)


time.sleep(500)

