package com.serdar.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
/**
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic user-table --config cleanup.policy=compact
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases-enriched-left-join
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases-enriched-inner-join

  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic user-purchases-enriched-left-join \
    --from-beginning \
      --formatter kafka.tools.DefaultMessageFormatter \
      --property print.key=true \
      --property print.value=true \
      --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
      --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer


  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
      --topic user-purchases-enriched-inner-join \
      --from-beginning \
      --formatter kafka.tools.DefaultMessageFormatter \
      --property print.key=true \
      --property print.value=true \
      --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
      --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 *
 * KAFKA STRAM JOIN Example
 * 1- Read topic-1 as GlobalKtable
 * 2- Read topic-2 as KStream
 * 3- Join
 * 4- Left Join
 *
 * */
public class UserEventEnricherApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"user-event-enricher-app");//Application ID unique
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // we get a global table out of Kafka. This table will be replicated on each Kafka Streams application

        //the key of our globalTable is the user ID
        GlobalKTable<String,String> userGlobalTable = builder.globalTable("user-table");

        //we get a stram of purchases
        KStream<String,String> userPurchases = builder.stream("user-purchases");

        /**
         * islmler KStream ile okunan topic'e gore yapildigindan,
         * stream'in run time da degisiklikleri algilamasi icin KStream topicinde yani  user-purchases'de biseyler olmasi gerekmektedir. (publish)
         * sadece GlobalKTable topic'i olan "user-table"a veri gelmesi tetiklemeye yetmez.
         * inner join isleminde her iki topicteki keylerde de veri olmasi gerekir.
         * left-join isleminde ise join edilen kisimda veri olmasina gerek yoktur. Bu streamde join edilen GlobalKTable topici user-table'dir.
         *
         * */
        //We want to enrich (zenginlestirmek) that stream
        KStream<String,String> userPurchasesEnrichedJoin = userPurchases
                .join(  userGlobalTable,
                        (key,value) -> key,// map from the (key ,value) of this Kstream to key of the GlobalKTable
                                            /* KStream gelen key,value lambda sonucunda verilen GlobalKTable ile
                                            inner Join yapilacak. KStream key ile GlobalKTable key  inner joinde kullanilacak.*/
                        (userPurchase, userInfo)-> "Purchase=" + userPurchase+", UserInfo[" +userInfo+"]"); // value of stream,value of table

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        KStream<String,String> userPurchasesEnrichedLeftJoin =
                userPurchases.leftJoin(userGlobalTable,
                        (key,value)->key,// map from the (key ,value) of this Kstream to key of the GlobalKTable
                        (userPurchase,userInfo)->{
                            if (userInfo != null)
                                return "Purchase=" + userPurchase+", UserInfo[" +userInfo+"]";
                            else
                                return "Purchase=" + userPurchase+", UserInfo=null]";
                        });


        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
