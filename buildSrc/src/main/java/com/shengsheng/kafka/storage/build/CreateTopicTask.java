package com.shengsheng.kafka.storage.build;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class CreateTopicTask extends DefaultTask {


    @TaskAction
    public void createTopic() throws ExecutionException, InterruptedException {
        String topicName = (String) getProject().findProperty("topicName");
        String bootstrap = (String) getProject().findProperty("bootstrap");
        String partitions = (String) getProject().findProperty("partitions");
        Objects.requireNonNull(topicName,"topicName must not be null");
        Objects.requireNonNull(bootstrap,"bootstrap must not be null");
        Objects.requireNonNull(partitions,"partitions must not be null");
        Map<String, Object> adminConfig = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        try (AdminClient adminClient = AdminClient.create(adminConfig)) {

            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
            topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "1000");
            topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "3600000");
            topicConfig.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1048576");
            topicConfig.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "1000");

            NewTopic topic = new NewTopic(topicName, Integer.parseInt(partitions), (short) 1).configs(topicConfig);

            adminClient.createTopics(Collections.singletonList(topic)).all().get();
            System.out.println("Topic created successfully.");
        }
    }

}
