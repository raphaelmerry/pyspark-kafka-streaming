import findspark
findspark.init('/opt/apache-spark')
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from actors import pyProducer
import json


sc = pyspark.SparkContext \
            .getOrCreate(pyspark.SparkConf() \
            .setMaster("local[*]"))
sc.setLogLevel("FATAL")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 60)


def connectConsumer(topic):
    ''' Connect to Kafka stream '''
    consumer = None
    print("\n"+'-'*4+" Waiting for records "+'-'*4+"\n")
    try:
        consumer = KafkaUtils.createDirectStream(ssc, [topic], {"bootstrap.servers": "localhost:9092"})
    except Exception as ex:
        print('Encountered error consuming from Kafka:\n   {}'.format(str(ex)))
    finally:
        return consumer


def formatQ2(source, word, topic_list):
    ''' Format stream according to Q2 schema '''
    topics = []
    for topic, keywords in topic_list.items():
        current = [topic for keyword in keywords if keyword == word]
        if current:
            topics.append(current)
    topics = [item for sublist in topics for item in sublist]
    string = {'source': source, 'word': word, 'topics': topics}
    return json.dumps(string)


def formatQ3(source, topic):
    ''' Format stream according to Q3 schema '''
    string = {'source': source, 'topic': topic}
    return json.dumps(string)


def consumeStream(topic1):
    ''' Get stream from Kafka '''
    print('\nConsuming records from Kafka ({})...'.format(topic1))
    kafka_consumer = connectConsumer(topic1)
    parsed = kafka_consumer.map(lambda record: json.loads(record[1]))
    parsed.count().map(lambda n: 'Number of records: {}'.format(n)).pprint()
    parsed.pprint()
    return parsed


def processStream(parsed, topic2, topic3, topic_list, kafka_producer):
    ''' Process parsed stream and output to other topics '''
    topics = list(topic_list.keys())
    keywords = [item for sublist in topic_list.values() for item in sublist]
    q2_records = parsed.map(lambda row: formatQ2(row['source'], row['word'], topic_list) if row['word'] in keywords else None)\
                      .filter(lambda row: row)
    q2_records.pprint()
    q3_records = parsed.map(lambda row: formatQ3(row['source'], row['word']) if row['word'] in topics else None)\
                      .filter(lambda row: row)
    q3_records.pprint()
    q2_records.foreachRDD(lambda rdd: publishStream(rdd, kafka_producer, topic2))
    q3_records.foreachRDD(lambda rdd: publishStream(rdd, kafka_producer, topic3))


def publishStream(rdd, kafka_producer, topic):
    ''' Iterate over RDD to push records to Kafka '''
    print("Pushed to topic {}\n".format(topic))
    records = rdd.collect()
    for record in records:
        pyProducer.sendRecords(kafka_producer, topic, record)


def startStreaming(ssc):
    ''' Start Spart processing '''
    ssc.start()
    ssc.awaitTermination()
