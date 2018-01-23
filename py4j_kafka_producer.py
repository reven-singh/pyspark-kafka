
# coding: utf-8

# In[14]:

from pyspark import SparkContext, SparkConf
from py4j.java_gateway import Py4JNetworkError
from pyspark.sql import SQLContext

class KafkaProducer:
    def __init__(self,jvm,topic,bootstrapServers):
        self.jvm=jvm
        self.topic=topic
        self.bootstrapServers=bootstrapServers

    def create_kafka_params(self):
        self.jvm.java.lang.System.setProperty("java.security.auth.login.config",
                                         "/usr/hdp/current/kafka-broker/conf/kafka_client_jaas.conf")
        kafkaParams = self.jvm.java.util.HashMap()
        kafkaParams.put("bootstrap.servers",self.bootstrapServers);
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT")
        kafkaParams.put("acks", "all");
        kafkaParams.put("retries", 1);
        kafkaParams.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
        return kafkaParams

    def send(self,message):
        try:
            kafkaClientsHelper = self.jvm.com.github.kafka.clients.KafkaClientsPythonHelper()
            kafkaClientsHelper.createProducer(self.create_kafka_params())
            kafkaClientsHelper.send(self.topic, self.jvm.java.util.UUID.randomUUID().toString(), message)
        except Py4JNetworkError:
            print("No JVM listening")
        except Exception:
            print("Unable to publish message")
        finally:
            kafkaClientsHelper.shutdownProducer()
            

        


# In[ ]:




# In[ ]:



