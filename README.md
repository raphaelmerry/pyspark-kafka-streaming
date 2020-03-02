# Streaming and processing text files with Kafka and PySpark

This project aims to stream contents of text files inside a local directory to Apache Kafka, and process them in batch with Spark Streaming through the Python API.

## Requirements
### Software

To run this project, you must have the following installed on your device with the correct version:

* **Apache Kafka** == 2.4.0
* **Apache Spark** == 2.4.4
* **Python** == 3.7.3

### Python Libraries

This project is written entirely in Python. To make use of the softwares above, we need a few libraries to use them locally in Python:

* **Kafka-Python** ([Project Page](https://github.com/dpkp/kafka-python))
   Python wrapper made to communicate with Apache Kafka.
   ```
   pip install kafka-python
   ```
   
* **findSpark** ([Project Page](https://github.com/minrk/findspark))
   To allow Python to find your local Spark installation at runtime.
   ```
   pip install findspark
   ```
   
* **pyArrow** ([Project Page](https://arrow.apache.org/docs/python/install.html))
   Used to save streams as Parquet files.
   ```
   pip install pyarrow
   ```

## Usage
### Environment Setup

* Kafka:
   You will first of all need a working Kafka instance on your device. Follow the [Kafka Quickstart](https://kafka.apache.org/quickstart) section of the documentation to setup a single-node instance.
   You will need to create 3 distincts Kafka topics in order to run this project: *Q1*, *Q2* and *Q3*. Run this command at the root of the Kafka folder to create the topic *Q1*:
   ```
   bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Q1
   ```
   
* Spark:
   To use pyspark, you will need to install [Spark](https://spark.apache.org/downloads.html) on your device and note the installation path. This path is required by findspark to initialise PySpark at runtime:
   ```
   findspark.init('/opt/apache-spark')
   ```
   You will need to modify this path accordingly at the very beggining of actors/sparkConsumer.py.

### Run project
   To launch the project, you must use the following commands at the root in distinct terminals:
   ```
   spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 ./main1.py
   spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 ./main2.py
   spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 ./main3.py
   ```
   This will download the required dependancy *spark-streaming-kafka-0-8_2.11* for Spark 2.4.4. Depending on your version of Spark, you might need to adjust this command.
   The sample data is an excerpt from the Gutenberg Project composed of 18 files.
1. The first script will process the whole corpus and send each word to a Kafka topic Q1.
2. The second script will load a list of topic and monitor the streams from Q1. If a match is found, it will send data to either topic Q2 (keyword match) or Q3 (topic name match).
3. The third script consumes records from the last offset, and save them in Parquet format.
4. Data Analysis (WIP)
