from actors import sparkConsumer, pyProducer
import json


def getTopic(path):
    ''' Parse the list of topics '''
    file = path + "topic.txt"
    f = open(file, "r", encoding='latin1')
    contents = f.read()
    f.close()
    return json.loads(contents)


def main(path, topic1, topic2, topic3):
    topic_list = getTopic(path)
    print("Topics:\n"+json.dumps(topic_list, indent=2)+'\n')
    kafka_producer = pyProducer.connectProducer()
    parsed = sparkConsumer.consumeStream(topic1)
    sparkConsumer.processStream(parsed, topic2, topic3, topic_list, kafka_producer)
    sparkConsumer.startStreaming(sparkConsumer.ssc)
    if kafka_producer is not None:
        kafka_producer.close()


if __name__ == '__main__':
    print('\n'+'='*5+' SCRIPT 2 '+'='*5+'\n')
    path = "../Data/"
    topic1 = 'Q1'
    topic2 = 'Q2'
    topic3 = 'Q3'
    main(path, topic1, topic2, topic3)
