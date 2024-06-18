package org.fwx.d04;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName KafkaClient
 * @Description 可靠性配置
 *          级别：0：表示不需要确认，1：表示Leader确认，-1或all：表示所有ISR副本确认
 *          设置消息发送后的确认级别为"1"，表示只有当Leader副本成功接收并写入消息时，才认为发送成功
 *          -1 需要等待所有ISR副本确认，需要在server.properties中配置：例如 min.insync.replicas=2，值为2，则副本数需大于等于3
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

        // 可靠性配置
        // 其它级别：0：表示不需要确认，1：表示Leader确认，-1或all：表示所有ISR副本确认
        // 设置消息发送后的确认级别为"1"，表示只有当Leader副本成功接收并写入消息时，才认为发送成功
        // -1 需要等待所有ISR副本确认，需要在server.properties中配置：例如 min.insync.replicas=2，值为2，则副本数需大于等于3
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 2.创建生产者
        KafkaProducer producer = new KafkaProducer<>(properties);

        // 3.发送消息
        LocalTime now = LocalTime.now();
        for (int i = 0; i < 30; i++) {

            producer.send(new ProducerRecord<>("first", now.toString() + " " + i + "aa"), (recordMetadata, e) -> {
                if (e == null){
                    System.out.println("主题：" + recordMetadata.topic() + " 分区：" + recordMetadata.partition());
                }
            });

            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // 4.关闭生产者
        producer.close();
    }
}
