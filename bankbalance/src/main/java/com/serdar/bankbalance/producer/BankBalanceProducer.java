package com.serdar.bankbalance.producer;

import com.serdar.bankbalance.avro.UserMoney;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * create topic
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-transactions-topic
 * */
public class BankBalanceProducer {

    public  final static String TOPIC = "bank-transactions-topic";
    //public  final static String TOPIC = "AA";
    ScheduledExecutorService executorService = null;
    Producer<String,byte[]> producer = null;
    List<UserMoney> userMoneyList = null;
    public BankBalanceProducer(){
        try {
            this.producer = ProducerCreator.createProducer();
            this.executorService = Executors.newSingleThreadScheduledExecutor();
            this.userMoneyList = new ArrayList<>();
            createUsers();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    Runnable runnable = ()->{
        try{
            System.out.println("BankBalanceProducer.runnable()");
            createRecord();
        }catch (Exception e){
            e.printStackTrace();
        }

    };
    public static void main(String[] args) {
       BankBalanceProducer obj = new BankBalanceProducer();
       obj.executeService();
    }

    private  void createRecord() {
        System.out.println("BankBalanceProducer.createRecord() --begin");

        int publishCount = 0;
        while (publishCount < 100){
            UserMoney userMoney = userMoneyList.get(new Random().nextInt(userMoneyList.size())); // random selection
            userMoney.setTime(new Date().toString());
            ProducerRecord<String,byte[]> record  = new ProducerRecord<String,byte[]>(TOPIC,userMoney.getName().toString(),
                                                                                        AvroSerializer.serializeAvroMessage(userMoney,
                                                                                        UserMoney.class));
            producer.send(record);
            publishCount++;
        }
        System.out.println("BankBalanceProducer.createRecord() --end -- publishCount:  " + publishCount);
    }

    private void executeService() {
        try {
            executorService.scheduleAtFixedRate(runnable,5,1, TimeUnit.SECONDS);//every second
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    String[] userNames = {"A","B","C","D","E","F"};

    private void createUsers(){
        for(int i=0; i<20; i++){//total 120 transactions for 6 customers
             for (String userName:userNames) {
                UserMoney userMoney = new UserMoney();
                userMoney.setName(userName);
                int amount = 1 + (int)(Math.random() * 1000); // random amount 1-1001
                userMoney.setAmount(String.valueOf(amount));
                userMoney.setTime(new Date(/*Math.abs(System.currentTimeMillis() - (new Random().nextInt(10*24*60*60*1000)))*/).toString());//random date (10 day) RandomUtils.nextLong(10*24*60*60*1000))).toString()
                userMoneyList.add(userMoney);
            }
        }
    }

}
