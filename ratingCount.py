from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('App')
sc = SparkContext(conf = conf)

rdd = sc.textFile('file:////home/ubuntu/ApacheSpark/Spark-tutorial/movieRatings/ratings.txt')
rdd1 = rdd.map(lambda x : x.strip().split(',')[1])
count = rdd1.countByValue()
print(count)
print('done')
