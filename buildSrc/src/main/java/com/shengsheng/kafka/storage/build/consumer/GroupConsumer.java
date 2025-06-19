package com.shengsheng.kafka.storage.build.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import static com.shengsheng.kafka.storage.build.consumer.MainConsumerConfig.MULTI_PARTITION_TOPIC;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class GroupConsumer {

    private static final String GROUP_ID = "test-group-1";


    public static void main(String[] args) {
        Map<String, Object> config = MainConsumerConfig.config();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        for (int i = 0; i < 2; i++) {
            new Thread(() -> {
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
                    consumer.subscribe(List.of(MULTI_PARTITION_TOPIC));
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                        for (ConsumerRecord<String, String> record : records) {
                            System.out.println(Thread.currentThread().getName() + "  " + record.value());
                        }
                    }
                }
            }).start();
        }
        LockSupport.park();
    }
}
