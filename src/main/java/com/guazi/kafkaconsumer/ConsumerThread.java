package com.guazi.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;

/**
 * @author yuyongjun
 * @date 2018/7/10 22:57
 */

public class ConsumerThread extends Thread {

    private static KafkaConsumer<String,byte[]> kafkaConsumer;

    public ConsumerThread(KafkaConsumer<String,byte[]> kafkaConsumer,String threadName){
        this.kafkaConsumer = kafkaConsumer;
        super.setName(threadName);
    }

    @Override
    public void run() {
        while (true){
//            ConsumerRecords<String,byte[]> consumerRecords = kafkaConsumer.poll(100);
//            int i=0;
//            for(ConsumerRecord<String,String> item : consumerRecords){
//                System.out.println("Consumer Message:"+item.value()+",Partition:"+item.partition()+"Offset:"+item.offset());
//                long offset = item.offset();
//                int partition = item.partition();
//                String topic = item.topic();
//                TopicPartition topicPartition = new TopicPartition(topic, partition);
//                kafkaConsumer.committed(topicPartition);
//                System.out.println("partition: " + topicPartition + " submit offset: " + (offset + 1L) + " to consumer task");
//                break;





//            }
            try {
                ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(100);
                if (!records.isEmpty()) {
                    for (TopicPartition partition : records.partitions()) {
                        System.out.println(partition.partition()+"==============");
                        List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                            System.out.println("consume offset = "+record.offset()+" key = "+record.key()+", value = "+record.value()+" ThreadName:"+Thread.currentThread().getName());
                            kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(record.offset() + 1)));
//                            kafkaConsumer.seek(new TopicPartition(record.topic(),record.partition()),record.offset()+1);
//                            break;
                        }
                        //消费者自己手动提交消费的offest,确保消息正确处理后再提交
//                        long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//                        kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                    }
                }else{
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
            }
        }
    }



}
