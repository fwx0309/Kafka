# 1.d01 简单的生产者Demo

1. 类名 org.fwx.d01.KafkaClient
2. 简单的kafka生产者，发送数据
3. 分区发送数据操作



# 2.d02 自定义分区器

​	

# 3.d03 吞吐量配置



# 4.d04 数据可靠



# 5.d05 幂等性 生产者事务



# 6.生产经验——数据有序

1. kafka在1.x版本之前保证数据单分区有序，条件如下：**max.in.flight.requests.per.connection**=1（不需要考虑是否开启幂等性）。

2. kafka在1.x及以后版本保证数据单分区有序，条件如下：

   （1）未开启幂等性

   **max.in.flight.requests.per.connection需要设置为1。**

   （2）开启幂等性

   **max.in.flight.requests.per.connection需要设置小于等于5。**

   原因说明：因为在kafka1.x以后，启用幂等后，kafka服务端会缓存producer发来的最近5个request的元数据，故无论如何，都可以保证最近5个request的数据都是有序的。



# 7. d06 消费者

1. 简单的消费者

   org.fwx.d06.KafkaConsumerClient

2. 消费指定的分区

   org.fwx.d06.KafkaConsumerClientFromPartition

3. 消费者组 

   org.fwx.d06.group

4. 消费者分区分配策略

   org.fwx.d06.group

5. 自动提交offset

   org.fwx.d06.KafkaConsumerClientOffsetAuto

6. 手动提交offset

   org.fwx.d06.KafkaConsumerClientOffsetByHand

7. 指定offset消费

   org.fwx.d06.KafkaConsumerClientOffsetSeek



# 8.SpringbootKafka 

​	springboot整合kafka，简单的生产者消费者demo