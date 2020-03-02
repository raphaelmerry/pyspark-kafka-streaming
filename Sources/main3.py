from actors import sparkConsumer
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from glob import glob
import datetime
import json
import os


def checkOutDirectories(topic2, topic3):
    ''' Check that output directories exist '''
    dir_list = ['Out', 'Out/'+topic2, 'Out/'+topic3]
    for dir in dir_list:
        if not glob(path+dir): os.mkdir(path+dir)


def streamToParquet(rdd, topic, path):
    ''' Parse DStream to DF and save to .parquet '''
    records = rdd.collect()
    rows = [tuple(record.items()) for record in records]
    rows_rdd = sparkConsumer.sc.parallelize(rows)
    schema = None
    type = None
    if topic == "Q2":
        type = StructType([
            StructField("source", StringType(), True),
            StructField("word", StringType(), True),
            StructField("topics", StringType(), True)
        ])
        schema = rows_rdd.map(lambda x: Row(source=x[0][1], word=x[1][1], topics=x[2][1]))
    elif topic == "Q3":
        type = StructType([
            StructField("source", StringType(), True),
            StructField("topic", StringType(), True)
        ])
        schema = rows_rdd.map(lambda x: Row(source=x[0][1], topic=x[1][1]))
    else:
        print("Failed to save stream (topic {} not found).\n".format(topic))
        return
    if rows_rdd.isEmpty():
        print("RDD is empty, nothing to save.\n")
        return
    filename = datetime.datetime.now()
    filename = filename.strftime("%Y-%m-%d_%H:%M:%S")
    filepath = path+"Out/"+topic+"/"+str(filename)+".parquet"
    print("Saving data from {} to {}".format(topic, filepath))
    df = sparkConsumer.spark.createDataFrame(schema, type).toPandas()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filepath, compression='snappy')


def main(path, topic2, topic3):
    '''
    sparkConsumer uses the createDirectStream approach to read streams from Kafka,
    as opposed to the receiver-based approach (createStream).
    Hence, the consumer defined in sparkConsumer reads from the latest offset by default.
    '''
    checkOutDirectories(topic2, topic3)
    q2_parsed = sparkConsumer.consumeStream(topic2)
    q2_parsed.foreachRDD(lambda rdd: streamToParquet(rdd, topic2, path))
    q3_parsed = sparkConsumer.consumeStream(topic3)
    q3_parsed.foreachRDD(lambda rdd: streamToParquet(rdd, topic3, path))
    sparkConsumer.startStreaming(sparkConsumer.ssc)


if __name__ == '__main__':
    print('\n'+'='*5+' SCRIPT 3 '+'='*5+'\n')
    path = "../Data/"
    topic2 = 'Q2'
    topic3 = 'Q3'
    main(path, topic2, topic3)
