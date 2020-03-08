
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('App')
sc = SparkContext(conf = conf)

rdd = sc.textFile('file:///home/ubuntu/ApacheSpark/sample2.txt',20)
rdd1 = rdd.flatMap(lambda x : x.strip().split())
rdd2 = rdd1.map(lambda word: (word, 1))
count = rdd2.reduceByKey(lambda a, b: a + b)
print("*********")
print(count.collect())
print('done')
while True:
    pass
