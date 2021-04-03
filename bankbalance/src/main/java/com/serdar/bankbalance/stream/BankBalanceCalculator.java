package com.serdar.bankbalance.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.serdar.bankbalance.avro.UserMoney;
import com.serdar.bankbalance.consumer.AvroDeserilizer;
import com.serdar.bankbalance.producer.BankBalanceProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
/**
 * compute total money,
 * transcations count,
 * latest time an update was received.
 *
 * Create topic
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-balance-streams-application-topic --config cleanup.policy=compact
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-balance-stream-mid-topic
 *
 * Console Consumers
 *
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bank-transactions --from-beginning --property print.key=true --property print.value=true
 *
 *
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
 *     --topic topic bank-balance-stream-mid-topic \
 *     --from-beginning \
 *     --formatter kafka.tools.DefaultMessageFormatter \
 *     --property print.key=true \
 *     --property print.value=true \
 *     --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 *     --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 *
 *  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
 *       --topic topic bank-balance-streams-application \
 *       --from-beginning \
 *       --formatter kafka.tools.DefaultMessageFormatter \
 *       --property print.key=true \
 *       --property print.value=true \
 *       --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 *       --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 *
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic favourite-colour-output --formatter kafka.tools.DefaultMessageFormatter
 * --property print.key=true  --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 * --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 *
 *
 * -- Kafka Streams Steps --
 * 1- Read topc from Kafka using KStream
 * 2- GroupByKey, because topic already has the right key. No repartition happens.
 * 3- Aggregate to compute bank balance
 * 4- To in order to write Kafka
 *
 * -- compact topic olusturmak icin --
 *  bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-keys-and-colours --config cleanup.policy=compact
 *
 *
 * */

public class BankBalanceCalculator {

    public final static String STREAM_TOPIC = "bank-balance-streams-application-test-topic";
    public final static String STREAM_MID_TOPIC = "bank-balance-stream-mid-test-topic";
    //public final static String STREAM_TOPIC = "CC";
    //public final static String STREAM_MID_TOPIC = "BB";

    public static void main(String[] args) {

        try {
            Properties config = new Properties();
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-stream-test");
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//latest

            //config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            //config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArrayDeserializer.class.getName()); // byte array deserializer because of avro

            //we disable the cache to demonstrate all the steps involved in transformation.
            config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
            //Exactly one processing !!
            config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

            // JSON Serde
            final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
            final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
            final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer,jsonDeserializer);


            StreamsBuilder builder = new StreamsBuilder();
            KStream<String,byte[]> kstream = builder.stream(BankBalanceProducer.TOPIC,Consumed.with(Serdes.String(),Serdes.ByteArray()));// value-> byte array

            KStream<String,UserMoney> userBalanceKStream = kstream
                    .map((key, value)-> new KeyValue<String,UserMoney>(key, AvroDeserilizer.deserializeAvroMessage(value, UserMoney.class, UserMoney.SCHEMA$)));

            KStream<String,JsonNode> balanceKStream = userBalanceKStream.
                    map((key,value)-> new KeyValue<String,JsonNode>(key,returnObjectNode(0,
                            Integer.valueOf(value.getAmount().toString()),
                            value.getTime().toString())));
            balanceKStream.to(STREAM_MID_TOPIC,Produced.with(Serdes.String(),jsonSerde));

            KStream<String,JsonNode> jsonStream =  builder.stream(STREAM_MID_TOPIC,Consumed.with(Serdes.String(),jsonSerde) );
            //create initial json object for balances
            ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
            initialBalance.put("count",0);
            initialBalance.put("balance",0);
            //initialBalance.put("time", String.valueOf(new Date(0L).getTime()));
            initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

            KTable<String,JsonNode> bankBalance = jsonStream
                    .groupByKey(Grouped.with(Serdes.String(),jsonSerde))
                    .aggregate(
                            () -> initialBalance,
                            (key,transaction,balance)->  newBalance(transaction, balance),
                            Named.as("SerdarTest"),
                            Materialized.with(Serdes.String(),jsonSerde)
                    );
            bankBalance.toStream().to(STREAM_TOPIC,Produced.with(Serdes.String(),jsonSerde));

            KafkaStreams streams = new KafkaStreams(builder.build(),config);
            streams.cleanUp();
            streams.start();

            System.out.println(streams.toString());

            //Shutdown hook to correctly close the strams application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));



        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static JsonNode returnObjectNode(int count,int balance, String  time){
        ObjectNode ret= null;
        try {
            // Jun 13 2003 23:11:52.454 UTC ==> MMM dd yyyy HH:mm:ss.SS zz
            // Sat Apr 03 01:45:04 TRT 2021 ==> EEE MMM dd HH:mm:ss zz yyyy
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zz yyyy");
            ZonedDateTime zdt = ZonedDateTime.parse(time,dtf);
            Long epochTime = zdt.toInstant().toEpochMilli();

            ret = JsonNodeFactory.instance.objectNode();
            ret.put("count",0);
            ret.put("balance",balance);
            ret.put("time", Instant.ofEpochMilli(epochTime).toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  ret;
    }

    public  static JsonNode newBalance (JsonNode transaction, JsonNode balance){
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        try {
            System.out.println("newBalance -- begin");
            // Jun 13 2003 23:11:52.454 UTC ==> MMM dd yyyy HH:mm:ss.SS zz
            // Sat Apr 03 01:45:04 TRT 2021 ==> EEE MMM dd HH:mm:ss zz yyyy
//            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zz yyyy");
//            ZonedDateTime zdt = ZonedDateTime.parse(transaction.get("time").asText(),dtf);

            newBalance.put("count",balance.get("count").asInt() + 1);
            newBalance.put("balance",balance.get("balance").asInt()
                    + transaction.get("balance").asInt());

            //Long balanceEpoch = Long.valueOf(balance.get("time").asText());//Instant.parse(balance.get("time").asText()).toEpochMilli();
           // Long transactionEpoch = zdt.toInstant().toEpochMilli();  //Instant.parse(transaction.get("time").asText()).toEpochMilli();
            Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
            Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
            Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch,transactionEpoch));
            newBalance.put("time",newBalanceInstant.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("newBalance -- end");
        return newBalance;
    }
}
