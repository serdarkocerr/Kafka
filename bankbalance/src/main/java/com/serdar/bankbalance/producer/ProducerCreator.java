package com.serdar.bankbalance.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class ProducerCreator {

    public static Producer<String,byte []> createProducer(){
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG,"bankbalance-producer");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all"); //strongest producing gurantee
        config.put(ProducerConfig.RETRIES_CONFIG,"3");
        config.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true"); //ensure don't publish duplicate for Kafka 0.11 later

       return new KafkaProducer<String, byte[]>(config);

    }
}
