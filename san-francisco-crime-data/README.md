# San Francisco Crime Data

This project was developed with a focus on Spark Streaming.


## Running the project

First we need kafka working. We need to instantiate the kafka zookeeper and the kafka server. 
If you have the kafka bin as a source, you can run



Run the project locally with

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --master local[*] data_stream.py
```