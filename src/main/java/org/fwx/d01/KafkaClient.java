package org.fwx.d01;

import org.apache.kafka.clients.producer.*;

import java.time.LocalTime;
import java.util.Properties;

/**
 * @ClassName KafkaClient
 * @Description 第一个kafka客户端，发送消息
 *      分区创建命令：bin/kafka-topics.sh --zookeeper BD1:2181 --create --topic first --replication-factor 2 --partitions 3
 *      同步发送数据：在send方法后添加 .get()
 *
 *      java api分区操作：
 *      （1）指明partition的情况下，直接将指明的值作为partition值；例如partition=0，所有数据写入分区0
 *      （2）没有指明partition值但有key的情况下，将key的hash值与topic的partition数进行取余得到partition值；
 *          例如：key1的hash值=5， key2的hash值=6 ，topic的partition数=2，那么key1 对应的value1写入1号分区，key2对应的value2写入0号分区。
 *      （3）既没有partition值又没有key值的情况下，Kafka采用Sticky Partition（黏性分区器），会随机选择一个分区，并尽可能一直
 *          使用该分区，待该分区的batch已满或者已完成，Kafka再随机一个分区进行使用（和上一次的分区不同）。
 *          例如：第一次随机选择0号分区，等0号分区当前批次满了（默认16k）或者linger.ms设置的时间到， Kafka再随机一个分区进行使用（如果还是0会继续随机）。
 * @Author Fwx
 * @Date 2024/6/17 10:43
 * @Version 1.0
 */
public class KafkaClient {
    public static void main(String[] args) {
        // 1.kafka 配置
        Properties properties = new Properties();

        // kafka集群地址 两种写法
        //properties.put("bootstrap.servers", "BD1:9092,BD2:9092,BD3:9092");
        //properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "BD1:9092,BD2:9092,BD3:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2.创建生产者
        KafkaProducer producer = new KafkaProducer<>(properties);

        // 3.发送消息
        LocalTime now = LocalTime.now();
        for (int i = 0; i < 3; i++) {
            // 1).数据没有指定分区
            /*producer.send(new ProducerRecord<>("first", now.toString() + " " + i), (recordMetadata, e) -> {
                if (e == null){
                    System.out.println("主题：" + recordMetadata.topic() + " 分区：" + recordMetadata.partition());
                }
            });*/

            // 2). 数据指定分区 0，需要设置key
            /*producer.send(new ProducerRecord<>("first", 0,"", now.toString() + " " + i), (recordMetadata, e) -> {
                if (e == null){
                    System.out.println("主题：" + recordMetadata.topic() + " 分区：" + recordMetadata.partition());
                }
            });*/

            // 3). 数据没有指定分区，key值hash分区，所以会随机选择一个分区
            producer.send(new ProducerRecord<>("first", "k1",now.toString() + " " + i), (recordMetadata, e) -> {
                if (e == null){
                    System.out.println("主题：" + recordMetadata.topic() + " 分区：" + recordMetadata.partition());
                }
            });
        }

        // 4.关闭生产者
        producer.close();
    }
}
