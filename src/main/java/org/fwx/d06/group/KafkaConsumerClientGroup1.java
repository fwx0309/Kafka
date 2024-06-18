package org.fwx.d06.group;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @ClassName KafkaComsumerClient
 * @Description 简单的消费者
 * @Author Fwx
 * @Date 2024/6/18 9:35
 * @Version 1.0
 */
public class KafkaConsumerClientGroup1 {
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

        /**
         * 配置分区分配策略。七个分区，三个消费者
         * 1、Range：首先对同一个 topic 里面的分区按照序号进行排序，并对消费者按照字母顺序进行排序。
         *      如：p1:0/1/2，p2:3/4，p3:5/6
         * 2、RoundRobin：针对集群中所有Topic而言。是把所有的 partition 和所有的consumer 都列出来，然后按照 hashcode 进行排序，最后
         * 通过轮询算法来分配 partition 给到各个消费者。
         *      如：p1:0/3/6，p2:1/4，p3:2/5
         * 3、Sticky：粘性分区定义：可以理解为分配的结果带有“粘性的”。即在执行一次新的分配之前，考虑上一次分配的结果，尽量少的调整分配的变动，可以节省大量的开销。
         */
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.RoundRobinAssignor");

        // 2. 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3. 订阅主题
        consumer.subscribe(Arrays.asList("first"));

        try {
            // 4. 消费消息
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    System.out.println(record.topic() + ":" + record.partition() + ":" +
                            record.offset() + ":" + record.key() + ":" + record.value());
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
