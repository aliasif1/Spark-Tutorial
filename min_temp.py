from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('Example-3')
sc = SparkContext(conf = conf)

def getData(data):
    place = data.split()[0]
    temp = int(data.split()[2])
    return (place,temp)

rdd1 = sc.textFile('file.txt')
rdd2 = rdd1.filter(lambda x : 'MIN' in x.split()[1])
rdd3 = rdd2.map(getData)
rdd4 = rdd3.reduceByKey(lambda x,y:min(x,y))
for item in rdd4.collect():
    print(item)
