package com.shengsheng.kafka.storage.build.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class MainConsumerConfig {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static final String TOPIC = "test1";

    public static final String MULTI_PARTITION_TOPIC = "multi-partition-topic";

    public static Map<String, Object> config() {
        Map<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        map.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return map;
    }

}
