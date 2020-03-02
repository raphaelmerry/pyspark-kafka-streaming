from actors import sparkConsumer
import pyarrow.parquet as pq
from pyspark.sql import Row
import datetime
import json


def streamToParquet(rdd, topic, path):
    ''' Parse DStream to DF and save to .parquet '''
    records = rdd.collect()
    rows = [tuple(record.items()) for record in records]
    rows_rdd = sparkConsumer.sc.parallelize(rows)
    schema = None
    if topic == "Q2":
        schema = rows_rdd.map(lambda x: Row(source=x[0][1], word=x[1][1], topics=x[2][1]))
    elif topic == "Q3":
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
    df = sparkConsumer.sqlContext.createDataFrame(schema)
    df.show()
    # PyArrow implementation not working yet
    # pq.write_table(df, filepath, compression='snappy', flavor='spark')
    df.write.parquet(filepath)


def main(path, topic2, topic3):
    '''
    sparkConsumer uses the createDirectStream approach to read streams from Kafka,
    as opposed to the receiver-based approach (createStream).
    According to the Spark documentation:
    The direct approach "periodically queries Kafka for the latest offsets in each topic+partition,
    and accordingly defines the offset ranges to process in each batch".
    Hence, the consumer defined in sparkConsumer reads from the last offset by default.
    '''
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
