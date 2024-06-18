package org.fwx.d05;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalTime;
import java.util.Properties;

/**
 * @ClassName KafkaClient
 * @Description 幂等性
 *          幂等性就是指Producer不论向Broker发送多少次重复数据，Broker端都只会持久化一条，保证了不重复。
 *          精确一次（Exactly Once） = 幂等性 + 至少一次（ ack=-1 + 分区副本数>=2 + ISR最小副本数量>=2） 。
 *
 *          开启参数 enable.idempotence 默认为 true，false 关闭
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

        // *** 设置事务 id（必须），事务 id 任意起名
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional_id_01");

        // 2.创建生产者
        KafkaProducer producer = new KafkaProducer<>(properties);

        // 2.1 初始化事务
        producer.initTransactions();
        // 2.2 开启事务
        producer.beginTransaction();

        try {
            // 3.发送消息
            LocalTime now = LocalTime.now();
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>("first1", now.toString() + " " + i + "aa"), (recordMetadata, e) -> {
                    if (e == null){
                        System.out.println("主题：" + recordMetadata.topic() + " 分区：" + recordMetadata.partition());
                    }
                });

                // 模拟异常
//                if (i == 2){
//                    int a = 1 / 0;
//                }
            }
            // 2.3 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            // 2.4 回滚事务
            producer.abortTransaction();
            throw new RuntimeException(e);
        } finally {
            // 4.关闭生产者
            producer.close();
        }


    }
}
