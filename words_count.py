import sys
import time
from pyspark import SparkContext, SparkConf

input = sys.argv[1]
output = sys.argv[2]

if __name__ == "__main__":
    start_time = time.time()
    sc = SparkContext(conf=SparkConf())
    words = sc.textFile(input).flatMap(lambda line: line.split())
    wordCounts = words.map(lambda word: (word, 1)
                           ).reduceByKey(lambda a, b: a + b)
    wordCounts.saveAsTextFile(output)
    print("--- %s seconds ---" % (time.time() - start_time))
