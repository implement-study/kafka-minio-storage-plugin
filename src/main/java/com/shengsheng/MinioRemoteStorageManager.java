package com.shengsheng;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.UploadObjectArgs;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class MinioRemoteStorageManager implements RemoteStorageManager {


    private MinioClient minioClient;

    private String bucketName;

    private final Map<TopicIdPartition, MinioTopicPartitionRemoteLogFinder> topicFinder = new ConcurrentHashMap<>();

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata, LogSegmentData logSegmentData) throws RemoteStorageException {
        try {
            System.out.println("开始copyLogSegmentData");
            uploadLocalPathToMinio(remoteLogSegmentMetadata, logSegmentData.logSegment());
            uploadLocalPathToMinio(remoteLogSegmentMetadata, logSegmentData.offsetIndex());
            uploadLocalPathToMinio(remoteLogSegmentMetadata, logSegmentData.timeIndex());
            if (logSegmentData.transactionIndex().isPresent()) {
                uploadLocalPathToMinio(remoteLogSegmentMetadata, logSegmentData.transactionIndex().get());
            }
            uploadLocalPathToMinio(remoteLogSegmentMetadata, logSegmentData.producerSnapshotIndex());
            uploadLocalPathToMinio(remoteLogSegmentMetadata, logSegmentData.leaderEpochIndex(), "leader-epoch" +
                "-checkpoint");
            safeGetFinder(remoteLogSegmentMetadata).put(logSegmentData.logSegment().getFileName().toString());
        } catch (Exception e) {
            throw new RemoteStorageException(e);
        }
        return Optional.empty();
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition) throws RemoteStorageException {
        System.out.println("开始fetchLogSegment");
        MinioTopicPartitionRemoteLogFinder finder = safeGetFinder(remoteLogSegmentMetadata);
        String fileName = finder.floor(startPosition);
        String dir = topicDirFromMetadata(remoteLogSegmentMetadata);
        try {
            return minioClient.getObject(
                GetObjectArgs.builder()
                    .bucket(bucketName)
                    .object(dir + "/" + fileName)
                    .offset((long) startPosition)
                    .build());
        } catch (Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition,
                                       int endPosition) throws RemoteStorageException {
        System.out.println("开始fetchLogSegment");
        MinioTopicPartitionRemoteLogFinder finder = safeGetFinder(remoteLogSegmentMetadata);
        String fileName = finder.floor(remoteLogSegmentMetadata.startOffset());
        String dir = topicDirFromMetadata(remoteLogSegmentMetadata);
        try {
            return minioClient.getObject(
                GetObjectArgs.builder()
                    .bucket(bucketName)
                    .object(dir + "/" + fileName)
                    .offset((long) startPosition)
                    .length((long) (endPosition - startPosition))
                    .build());
        } catch (Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata, IndexType indexType) throws RemoteStorageException {
        System.out.println("开始fetchIndex");
        MinioTopicPartitionRemoteLogFinder finder = safeGetFinder(remoteLogSegmentMetadata);
        String fileName = finder.floor(remoteLogSegmentMetadata.startOffset());
        String indexName = fileName.substring(0, fileName.length() - 4);
        switch (indexType) {
            case OFFSET -> indexName = indexName.concat(".index");
            case TIMESTAMP -> indexName = indexName.concat(".timeindex");
            case TRANSACTION -> indexName = indexName.concat(".txnindex");
            case PRODUCER_SNAPSHOT -> indexName = indexName.concat(".snapshot");
            case LEADER_EPOCH -> indexName = "leader-epoch-checkpoint";
        }
        String dir = topicDirFromMetadata(remoteLogSegmentMetadata);
        try {
            return minioClient.getObject(
                GetObjectArgs.builder()
                    .bucket(bucketName)
                    .object(dir + "/" + indexName)
                    .build());
        } catch (Exception e) {
            System.out.println("load " + remoteLogSegmentMetadata.topicIdPartition().topic() + ".type:" + indexType);
            return InputStream.nullInputStream();
        }
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        TopicIdPartition topicIdPartition = remoteLogSegmentMetadata.topicIdPartition();
        String topic = topicIdPartition.topic();
    }

    @Override
    public void close() throws IOException {
        try {
            minioClient.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("初始化minio啦啦啦啦啦");
        minioClient = MinioClient.builder()
            .endpoint("http://localhost:9000")
            .credentials("minioadmin", "minioadmin")
            .build();

        bucketName = "kafka-data";
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void uploadLocalPathToMinio(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Path localPath) throws Exception {
        String dir = topicDirFromMetadata(remoteLogSegmentMetadata);
        minioClient.uploadObject(
            UploadObjectArgs.builder()
                .bucket(bucketName)
                .object(dir + "/" + localPath.getFileName().toString())
                .filename(localPath.toAbsolutePath().toString())
                .build());
    }

    private void uploadLocalPathToMinio(RemoteLogSegmentMetadata remoteLogSegmentMetadata, ByteBuffer buffer,
                                        String fileName) throws Exception {

        String dir = topicDirFromMetadata(remoteLogSegmentMetadata);

        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        minioClient.putObject(
            PutObjectArgs.builder()
                .bucket(bucketName)
                .object(dir + "/" + fileName)
                .stream(inputStream, data.length, -1)
                .contentType("application/octet-stream")
                .build()
        );
    }

    private MinioTopicPartitionRemoteLogFinder safeGetFinder(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return topicFinder.computeIfAbsent(remoteLogSegmentMetadata.topicIdPartition(),
            tip -> new MinioTopicPartitionRemoteLogFinder(minioClient, tip, bucketName));
    }

    public static String topicDirFromMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        TopicIdPartition topicIdPartition = remoteLogSegmentMetadata.topicIdPartition();
        return topicIdPartition.topic() + "-" + topicIdPartition.partition();
    }
}
