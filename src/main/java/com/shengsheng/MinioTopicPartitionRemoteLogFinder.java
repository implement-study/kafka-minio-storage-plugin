package com.shengsheng;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.apache.kafka.common.TopicIdPartition;

import java.util.concurrent.ConcurrentSkipListMap;

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
                    .prefix(topicIdPartition.topic() + "-" + topicIdPartition.partition() + "/")
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

    public String floor(long startOffset) {
        return remoteLogSegmentMap.floorEntry(startOffset).getValue();
    }

}
