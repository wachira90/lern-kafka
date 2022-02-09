#!python

from __future__ import print_function
import sys
from pyspark import SparkContext, SparkConf 
from pyspark.streaming import StreamingContext 
from pyspark.streaming.kafka import KafkaUtils

checkpoint = "hdfs://ns1/user/systest/checkpoint"
# Function to create and setup a new StreamingContext 
def functionToCreateContext():

  sparkConf = SparkConf()  
  sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")  
  sc = SparkContext(appName="PythonStreamingKafkaWordCount",conf=sparkConf)  
  ssc = StreamingContext(sc, 10)

  zkQuorum, topic = sys.argv[1:]  
  kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})  
  lines = kvs.map(lambda x: x[1])  
  counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)  
  counts.pprint()

  ssc.checkpoint(checkpoint)   # set checkpoint directory  
  return ssc

if __name__ == "__main__":  
    if len(sys.argv) != 3:    
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)    
        exit(-1)

    ssc = StreamingContext.getOrCreate(checkpoint, lambda: functionToCreateContext())  
    ssc.start()  
    ssc.awaitTermination()