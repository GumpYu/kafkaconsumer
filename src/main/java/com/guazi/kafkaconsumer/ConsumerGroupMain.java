package com.guazi.kafkaconsumer;

import com.guazi.kafkaconsumer.utils.IpUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author yuyongjun
 * @date 2018/7/10 23:00
 */

public class ConsumerGroupMain {

    public static void main(String[] args) {

        String address = IpUtils.getLocalIp4Addr();
        System.out.println("IP "+address);
        String brokers = address+ ":9092,"+address+":9093,"+address+":9094";
        String groupId = "group01";
//        String topic = "mytopic";
        String topic = "my-repli";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "false");
//        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("max.poll.records", "1");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<String, byte[]>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));

        int consumerNumber = 3;


        ConsumerGroup consumerGroup = new ConsumerGroup(kafkaConsumer, consumerNumber);
        consumerGroup.start();
    }
}
