
# coding: utf-8

# In[1]:

class KafkaProducer:
    def __init__(self,sc,bootstrap_servers):
        self.scontext=sc
        props=self.scontext._jvm.java.util.Properties()
        props.put("bootstrap.servers", bootstrap_servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol","SASL_PLAINTEXT");
        props.put("sasl.jaas.config", 
        "com.sun.security.auth.module.Krb5LoginModule required " +
        "useKeyTab=\"false\" " +
        "useTicketCache=\"true\" " + 
        "serviceName=\"kafka\";");


        self.kafkaProducer = self.scontext._jvm.org.apache.kafka.clients.producer.KafkaProducer(props)
        return

    def publishToTopic(self,topic,msg):
    
        self.kafkaProducer.send(self.scontext._jvm.org.apache.kafka.clients.producer.ProducerRecord(topic, msg));
        return

    


# In[ ]:




# In[ ]:



