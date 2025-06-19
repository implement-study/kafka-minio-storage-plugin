package com.shengsheng.kafka.storage.build;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class ProducerTask extends DefaultTask {


    @TaskAction
    public void produce() throws IOException, InterruptedException, ExecutionException {
        String bootstrap = (String) getProject().findProperty("bootstrap");
        String topicName = (String) getProject().findProperty("topicName");
        String count = (String) getProject().findProperty("count");
        String delayMillis = (String) getProject().findProperty("delayMillis");
        String repeat = (String) getProject().findProperty("repeat");
        Objects.requireNonNull(topicName, "topicName must not be null");
        Objects.requireNonNull(bootstrap, "bootstrap must not be null");
        Objects.requireNonNull(count, "count must not be null");
        Objects.requireNonNull(delayMillis, "delayMillis must not be null");
        Objects.requireNonNull(repeat, "repeat must not be null");
        Map<String, Object> producerConfig = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        Path seq = Path.of(getProject().getBuildDir().toPath().toAbsolutePath().toString(), "tmp", "seq", topicName);
        if (!seq.toFile().exists()) {
            seq.toFile().getParentFile().mkdirs();
            Files.write(seq, ByteBuffer.allocate(Integer.BYTES).putInt(0).array());
        }


        int partitionCount;
        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            DescribeTopicsResult result = admin.describeTopics(List.of(topicName));
            TopicDescription description = result.topicNameValues().get(topicName).get();
            partitionCount = description.partitions().size();
        }

        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig)) {
            int repeatTime = Integer.parseInt(repeat);
            int sendCount = Integer.parseInt(count);
            System.out.println("Start to produce " + repeatTime + " batches, each batch contains " + sendCount + " messages ...");
            for (int batchCount = 0; batchCount < repeatTime; batchCount++) {
                int toPartition = batchCount % partitionCount;
                int offset = ByteBuffer.wrap(Files.readAllBytes(seq)).getInt();
                for (int i = 0; i < sendCount; i++) {
                    producer.send(new ProducerRecord<>(topicName, toPartition, null, String.valueOf(offset + i).getBytes()));
                }
                producer.flush();
                Files.write(seq, ByteBuffer.allocate(Integer.BYTES).putInt(offset + sendCount).array());
                System.out.printf("send [%d] - [%d] to %s  partition[%d] %n", offset, offset + sendCount, topicName, toPartition);
                Thread.sleep(Long.parseLong(delayMillis));
            }
        }

    }
}
