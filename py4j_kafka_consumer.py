
# coding: utf-8

# In[ ]:

from pyspark import SparkContext, SparkConf
from py4j.java_gateway import Py4JNetworkError
from pyspark.sql import SQLContext

class KafkaConsumer:
    def __init__(self,jvm,topic,bootstrapServers):
        self.jvm=jvm
        self.topic=topic
        self.bootstrapServers=bootstrapServers
        self.kafkaClientsHelper=self.jvm.com.github.kafka.clients.KafkaClientsPythonHelper()
        self.kafkaClientsHelper.createConsumer(self.create_kafka_params())
        self.kafkaClientsHelper.subscribe(self.topic)

    def create_kafka_params(self):
        #self.jvm.java.lang.System.setProperty("java.security.auth.login.config",
         #                                "/usr/hdp/current/kafka-broker/conf/kafka_client_jaas.conf")
        kafkaParams = self.jvm.java.util.HashMap()
        kafkaParams.put("bootstrap.servers",self.bootstrapServers);
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT")
        kafkaParams.put("acks", "all");
        kafkaParams.put("retries", 1);
        kafkaParams.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "test");
        kafkaParams.put("enable.auto.commit", "true");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("session.timeout.ms", "30000");
        kafkaParams.put("sasl.jaas.config", 
        "com.sun.security.auth.module.Krb5LoginModule required " +
        "useKeyTab=\"false\" " +
        "useTicketCache=\"true\" " + 
        "serviceName=\"kafka\";");
        return kafkaParams

    def consume(self):
        records=self.kafkaClientsHelper.consume()
        return records

