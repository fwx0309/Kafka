package org.fwx.d02;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @ClassName MyPartitioner
 * @Description 自定义分区器
 * @Author Fwx
 * @Date 2024/6/17 15:23
 * @Version 1.0
 */
public class MyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String data = value.toString();
        if (data.contains("aa")){
            return 0;
        }else if (data.contains("bb")){
            return 1;
        }else if (data.contains("cc")){
            return 2;
        }
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
