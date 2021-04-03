package com.serdar.bankbalance.consumer;


import com.serdar.bankbalance.avro.UserMoney;
import com.serdar.bankbalance.producer.BankBalanceProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BankBalanceConsumer {
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    public static Consumer<String, byte[]> consumer = null;

    public BankBalanceConsumer() {
        try {
            consumer = ConsumerCreator.createConsumer();
            consumer.subscribe(Collections.singletonList(BankBalanceProducer.TOPIC));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        BankBalanceConsumer bankBalanceConsumer = new BankBalanceConsumer();
        bankBalanceConsumer.executeService( () -> {
            consume();
        });
    }

    private static void consume() {
        try {
            System.out.println("<<<<-------- consume ----------->>>> BEGIN");
            ConsumerRecords<String,byte[]> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(p->{
                UserMoney userMoney = AvroDeserilizer.deserializeAvroMessage(p.value(), UserMoney.class, UserMoney.SCHEMA$);
                System.out.println("consume topic : " + " |Name: " + userMoney.getName()  + " |Amount: " + userMoney.getAmount() + " |Date: " + userMoney.getTime());
            });

            System.out.println("<<<<-------- consume ----------->>>> END");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void executeService(Runnable runnable) {
        try {
            executorService.scheduleAtFixedRate(runnable,5,10, TimeUnit.SECONDS);//every 10 second
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
