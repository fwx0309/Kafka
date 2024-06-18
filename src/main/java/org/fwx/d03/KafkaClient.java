package org.fwx.d03;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalTime;
import java.util.Properties;

/**
 * @ClassName KafkaClient
 * @Description 吞吐量配置
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
        // *** 吞吐量配置 ***
        // batch.size：批次大小，默认 16K。生产环境16K-32K，生产环境32K-64K，生产环境64K-128K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // linger.ms：等待时间，默认 0。生产环境1ms-5ms，生产环境10ms-30ms，生产环境30ms-60ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // RecordAccumulator：缓冲区大小，默认 32M：buffer.memory。生产环境 32M-64M，生产环境 64M-128M，生产环境 128M-256M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        // compression.type：压缩，默认 none
        // 可配置值 gzip、snappy、lz4 和 zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");


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
