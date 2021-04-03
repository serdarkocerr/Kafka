package com.serdar.bankbalance.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ConsumerCreator {
    final static String grupId = "TestGruop" + Long.toString(System.currentTimeMillis());
    public static Consumer<String,byte []> createConsumer(){
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG,grupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
       // config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,1);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        Consumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(config);

        return consumer;
    }
}
