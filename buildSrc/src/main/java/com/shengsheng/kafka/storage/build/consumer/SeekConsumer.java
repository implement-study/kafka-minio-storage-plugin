package com.shengsheng.kafka.storage.build.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.shengsheng.kafka.storage.build.consumer.MainConsumerConfig.TOPIC;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class SeekConsumer {

    private static final int OFFSET = 3658;

    public static void main(String[] args) {
        Map<String, Object> config = MainConsumerConfig.config();
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            System.out.println("consumer start...");
            TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, OFFSET);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
            }
        }
    }
}
