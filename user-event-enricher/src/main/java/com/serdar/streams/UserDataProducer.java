package com.serdar.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {
    public static void main(String[] args) {
        try {
            Properties config = new Properties();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
            config.put(ProducerConfig.CLIENT_ID_CONFIG,"userdata-producer");
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            config.put(ProducerConfig.ACKS_CONFIG, "all"); //strongest producing gurantee
            config.put(ProducerConfig.RETRIES_CONFIG,"3");
            config.put(ProducerConfig.LINGER_MS_CONFIG,"1");

            config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true"); //ensure don't publish duplicate for Kafka 0.11 later
            Producer<String,String> producer = new KafkaProducer<>(config);

            Scanner sc = new Scanner(System.in);

            //1- Create new user, then some data to Kafka
            //Expecting inner join show purchase=Apples and Bananas (1) userInfo="First=John,Last=Doe,Email=john.doe@gmail.com"
            // leftjoin show same inner join
            System.out.println("\nExample 1 - new user\n");
            producer.send(userRecord("john","First=John,Last=Doe,Email=john.doe@gmail.com")).get();
            producer.send(purchaseRecord("john","Apples and Bananas (1)")).get();

            Thread.sleep(10000);

            //2- Receive user purchase but it doesnt exist in Kafka
            // Expecting inner join not publish
            //Expecting left join show purchase="Kafka Udemy Course (2)" userInfo=null
            System.out.println("\nExample 2 - non existing user\n");
            producer.send(purchaseRecord("bob","Kafka Udemy Course (2)")).get();

            Thread.sleep(10000);
            //3- Update user "john" and send ne transaction
            System.out.println("\nExample 3 - update to user\n");
            producer.send(userRecord("john","First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
            producer.send(purchaseRecord("john","Oranges (3)")).get();

            Thread.sleep(10000);
            //4- Send a user purchase for serdar but it exist in Kafka later
            System.out.println("\nExample 4 - note existing user then user\n");
            producer.send(purchaseRecord("serdar","Computer (4)"));
            producer.send(userRecord("serdar","First=Serdar,Last=Kocer,Github=serdarkocerr")).get();
            producer.send(purchaseRecord("serdar","Books (4)"));
            producer.send(userRecord("serdar",null)).get();//deleting data
            Thread.sleep(10000);

            //5- create a user but it gets deleted before any purchase comes through
            System.out.println("\nExample 5 - user then delete then data\n");
            producer.send(userRecord("alice","First=Alice")).get();
            producer.send(userRecord("alice",null)).get();//deleted.
            producer.send(purchaseRecord("alice","Apache Kafka Series (5)"));
            Thread.sleep(10000);


            System.out.println("End of demo");
            producer.close();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static ProducerRecord<String,String> userRecord(String key, String value){
        return  new ProducerRecord<>("user-table", key, value);
    }

    private static ProducerRecord<String,String> purchaseRecord(String key, String value){
        return  new ProducerRecord<>("user-purchases", key, value);
    }
}
