package org.fwx.d06;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @ClassName KafkaComsumerClient
 * @Description 指定时间偏移量消费数据
 * @Author Fwx
 * @Date 2024/6/18 9:35
 * @Version 1.0
 */
public class KafkaConsumerClientOffsetSeekTime {
    public static void main(String[] args) {
        // 1. 配置参数
        Properties properties = new Properties();
        // 配置kafka集群地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"BD1:9092,BD2:9092,BD3:9092");
        // 配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"first-group-id");
        // 配置反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");


        // 2. 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3. 订阅主题
        consumer.subscribe(Arrays.asList("first"));

        /**
         * 指定时间位置消费数据
         */
        // 获取分区
        Set<TopicPartition> assignment = consumer.assignment();
        while (assignment.isEmpty()){
            consumer.poll(Duration.ofSeconds(1));
            assignment = consumer.assignment();
        }

        // 希望把时间转换为对应的offset
        HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();
        // 封装对应集合
        for (TopicPartition topicPartition : assignment) {
            topicPartitionLongHashMap.put(topicPartition,System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }

        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(topicPartitionLongHashMap);

        // 指定消费的offset
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
            consumer.seek(topicPartition,offsetAndTimestamp.offset());
        }

        try {
            // 4. 消费消息
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    System.out.println(record.topic() + ":" + record.partition() + ":" + record.offset() + ":" + record.key() + ":" + record.value());
                });

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // 5. 关闭消费者
            consumer.close();
        }
    }
}
