import sys
from pyspark import SparkContext, SparkConf
 
#input = "hdfs://localhost/user/ebrahimi/file500.txt"
input = sys.argv[0]
output = sys.argv[1]

if __name__ == "__main__":
	sc = SparkContext(conf=SparkConf())

	words = sc.textFile(input).flatMap(lambda line: line.split())
	wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	wordCounts.saveAsTextFile(output)