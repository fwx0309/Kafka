package org.fwx.d02;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalTime;
import java.util.Properties;

/**
 * @ClassName KafkaClient
 * @Description 使用自定义分区器
 * @Author Fwx
 * @Date 2024/6/17 10:43
 * @Version 1.0
 */
public class KafkaClient {
    public static void main(String[] args) {
        // 1.kafka 配置
        Properties properties = new Properties();

        // kafka集群地址 两种写法
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "BD1:9092,BD2:9092,BD3:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.fwx.d02.MyPartitioner");

        // 2.创建生产者
        KafkaProducer producer = new KafkaProducer<>(properties);

        // 3.发送消息
        LocalTime now = LocalTime.now();
        for (int i = 0; i < 3; i++) {

            producer.send(new ProducerRecord<>("first", now.toString() + " " + i + "aa"), (recordMetadata, e) -> {
                if (e == null){
                    System.out.println("主题：" + recordMetadata.topic() + " 分区：" + recordMetadata.partition());
                }
            });
        }

        // 4.关闭生产者
        producer.close();
    }
}
