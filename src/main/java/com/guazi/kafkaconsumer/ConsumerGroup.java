package com.guazi.kafkaconsumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yuyongjun
 * @date 2018/7/10 22:59
 */

public class ConsumerGroup {

    private final int consumerNumber;
    private KafkaConsumer<String,byte[]> kafkaConsumer;
    private List<ConsumerThread> consumerThreadList = new ArrayList<ConsumerThread>();

    public ConsumerGroup(KafkaConsumer<String,byte[]> kafkaConsumer,int consumerNumber){
        this.consumerNumber = consumerNumber;
        for(int i = 0; i< consumerNumber;i++){
            ConsumerThread consumerThread = new ConsumerThread(kafkaConsumer,"ConsumerGroup "+i);
            consumerThreadList.add(consumerThread);
        }
    }

    public void start(){
        for (ConsumerThread item : consumerThreadList){
            Thread thread = new Thread(item);
            thread.start();
        }
    }

}
