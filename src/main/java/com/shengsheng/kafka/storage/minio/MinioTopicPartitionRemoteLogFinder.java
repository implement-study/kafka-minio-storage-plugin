package com.shengsheng.kafka.storage.minio;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.apache.kafka.common.TopicIdPartition;

import java.util.concurrent.ConcurrentSkipListMap;

import static com.shengsheng.kafka.storage.minio.RemoteUtils.topicDirFromTopicIdPartition;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class MinioTopicPartitionRemoteLogFinder {

    private final MinioClient minioClient;
    private final TopicIdPartition topicIdPartition;
    private final ConcurrentSkipListMap<Long, String> remoteLogSegmentMap;
    private final String bucketName;

    public MinioTopicPartitionRemoteLogFinder(MinioClient client, TopicIdPartition topicIdPartition,
                                              String bucketName) {
        this.minioClient = client;
        this.topicIdPartition = topicIdPartition;
        this.remoteLogSegmentMap = new ConcurrentSkipListMap<>();
        this.bucketName = bucketName;
        init();
    }

    public void init() {
        try {
            Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder()
                    .bucket(bucketName)
                    .prefix(topicDirFromTopicIdPartition(topicIdPartition) + "/")
                    .build());
            for (Result<Item> result : results) {
                Item item = result.get();
                String name = item.objectName();
                if (name.endsWith(".log")) {
                    int i = name.lastIndexOf('/');
                    name = name.substring(i + 1);
                    String logSegmentName = name.substring(0, name.length() - 4);
                    remoteLogSegmentMap.put(Long.parseLong(logSegmentName), name);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void put(String logName) {
        remoteLogSegmentMap.put(Long.valueOf(logName.substring(0, logName.length() - 4)), logName);
    }

    public void remove(long offset) {
        this.remoteLogSegmentMap.keySet().removeIf(key -> key <= offset);
    }

    public String floor(long startOffset) {
        String message = String.format("topic[%s] files:[%s]", topicIdPartition.topic(), remoteLogSegmentMap.values());
        System.out.println(message);
        String resultName = remoteLogSegmentMap.floorEntry(startOffset).getValue();
        System.out.println("find offset " + startOffset + "name :" + resultName);
        return resultName;
    }

}
