import sys
from pyspark import SparkContext, SparkConf
 
#input = "hdfs://localhost/user/ebrahimi/file500.txt"
input = "sample.txt"
output = "output"

if __name__ == "__main__":
	conf = SparkConf()
	conf.setMaster('yarn')
	conf.setAppName('words count')
	sc = SparkContext(conf=conf)

	print("start")
	words = sc.textFile(input).flatMap(lambda line: line.split())
	print("read")
	wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	wordCounts.saveAsTextFile(output)
	print("write")