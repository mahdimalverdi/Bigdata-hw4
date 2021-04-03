import sys
from pyspark import SparkContext, SparkConf
 
#input = "hdfs://localhost/user/ebrahimi/file500.txt"
input = sys.argv[1]
output = sys.argv[2]

if __name__ == "__main__":
	print("start")
	sc = SparkContext(conf=SparkConf())
	words = sc.textFile(input).flatMap(lambda line: line.split())
	wordCounts = words.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a,b:a +b)
	wordCounts.saveAsTextFile(output)