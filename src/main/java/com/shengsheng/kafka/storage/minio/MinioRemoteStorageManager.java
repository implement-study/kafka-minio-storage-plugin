package com.shengsheng.kafka.storage.minio;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.UploadObjectArgs;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.shengsheng.kafka.storage.minio.RemoteUtils.filenamePrefixFromOffset;
import static com.shengsheng.kafka.storage.minio.RemoteUtils.topicDirFromTopicIdPartition;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class MinioRemoteStorageManager implements RemoteStorageManager {

    private MinioClient minioClient;

    private String bucketName;

    private final Map<TopicIdPartition, MinioTopicPartitionRemoteLogFinder> topicFinderMap = new ConcurrentHashMap<>();

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata, LogSegmentData logSegmentData) throws RemoteStorageException {
        try {
            TopicIdPartition topicIdPartition = remoteLogSegmentMetadata.topicIdPartition();
            uploadLocalPathToMinio(topicIdPartition, logSegmentData.logSegment());
            uploadLocalPathToMinio(topicIdPartition, logSegmentData.offsetIndex());
            uploadLocalPathToMinio(topicIdPartition, logSegmentData.timeIndex());
            Optional<Path> txnIndex = logSegmentData.transactionIndex();
            if (txnIndex.isPresent()) {
                uploadLocalPathToMinio(topicIdPartition, txnIndex.get());
            }
            uploadLocalPathToMinio(topicIdPartition, logSegmentData.producerSnapshotIndex());
            uploadLeaderEpochIndex(remoteLogSegmentMetadata, logSegmentData.leaderEpochIndex());
            safeGetFinder(remoteLogSegmentMetadata).put(logSegmentData.logSegment().getFileName().toString());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RemoteStorageException(e);
        }
        return Optional.empty();
    }


    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition) throws RemoteStorageException {
        return fetchLogSegment(remoteLogSegmentMetadata, startPosition, remoteLogSegmentMetadata.segmentSizeInBytes());
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata, int startPosition,
                                       int endPosition) throws RemoteStorageException {
        String message = String.format("fetchLogSegment,start[%d]", metadata.startOffset());
        System.out.println(message);
        MinioTopicPartitionRemoteLogFinder finder = safeGetFinder(metadata);
        String fileName = finder.floor(metadata.startOffset());
        String dir = topicDirFromTopicIdPartition(metadata.topicIdPartition());
        try {
            System.out.printf("read bucket [%s] object[%s] ", bucketName, dir + "/" + fileName);
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
    public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata, IndexType indexType) throws RemoteStorageException {
        System.out.println("开始fetchIndex");
        long start = remoteLogSegmentMetadata.startOffset();
        long endOffset = remoteLogSegmentMetadata.endOffset();
        System.out.printf("fetch index [%s] start[%d] end[%d] %n", indexType, start, endOffset);
        MinioTopicPartitionRemoteLogFinder finder = safeGetFinder(remoteLogSegmentMetadata);
        String fileName = finder.floor(remoteLogSegmentMetadata.startOffset());
        String indexName = fileName.substring(0, fileName.length() - 4);
        switch (indexType) {
            case OFFSET -> indexName = indexName.concat(".index");
            case TIMESTAMP -> indexName = indexName.concat(".timeindex");
            case TRANSACTION -> indexName = indexName.concat(".txnindex");
            case PRODUCER_SNAPSHOT -> indexName = indexName.concat(".snapshot");
            case LEADER_EPOCH -> indexName = indexName.concat(".leader-epoch-checkpoint");
        }
        String dir = topicDirFromTopicIdPartition(remoteLogSegmentMetadata.topicIdPartition());
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
    public void deleteLogSegmentData(RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        try {
            TopicIdPartition tip = metadata.topicIdPartition();
            System.out.printf("deleteLogSegmentData topic %s partition %d start [%d] end [%d] %n",
                tip.topic(), tip.partition(), metadata.startOffset(), metadata.endOffset());
            String dir = topicDirFromTopicIdPartition(tip);
            long deleteStartOffset = metadata.startOffset();
            Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder()
                    .bucket(bucketName)
                    .prefix(dir + "/")
                    .build()
            );

            List<DeleteObject> objectsToDelete = new ArrayList<>();
            List<String> removeList = new ArrayList<>();
            for (Result<Item> result : results) {
                Item item = result.get();
                String objectName = item.objectName();
                String offsetName = objectName.substring(objectName.lastIndexOf("/") + 1, objectName.indexOf("."));
                long fileOffset = Long.parseLong(offsetName);
                if (fileOffset <= deleteStartOffset) {
                    objectsToDelete.add(new DeleteObject(item.objectName()));
                    removeList.add(objectName);
                }
            }
            if (objectsToDelete.isEmpty()) {
                return;
            }

            Iterable<Result<DeleteError>> deleteResult = minioClient.removeObjects(
                RemoveObjectsArgs.builder()
                    .bucket(bucketName)
                    .objects(objectsToDelete)
                    .build());
            for (Result<DeleteError> deleteErrorResult : deleteResult) {
                deleteErrorResult.get();
            }
            System.out.println("delete " + removeList);
            var finder = topicFinderMap.get(metadata.topicIdPartition());
            if (finder != null) {
                finder.remove(deleteStartOffset);
            }
        } catch (Exception e) {
            throw new RemoteStorageException(e);
        }

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

    private void uploadLocalPathToMinio(TopicIdPartition topicIdPartition, Path localPath) throws Exception {
        String dir = topicDirFromTopicIdPartition(topicIdPartition);
        minioClient.uploadObject(
            UploadObjectArgs.builder()
                .bucket(bucketName)
                .object(dir + "/" + localPath.getFileName().toString())
                .filename(localPath.toAbsolutePath().toString())
                .build());
    }

    private void uploadLeaderEpochIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata, ByteBuffer buffer) throws Exception {

        String dir = topicDirFromTopicIdPartition(remoteLogSegmentMetadata.topicIdPartition());
        String suffix = ".leader-epoch-checkpoint";
        byte[] data = new byte[buffer.remaining()];
        String filename = filenamePrefixFromOffset(remoteLogSegmentMetadata.startOffset());
        buffer.get(data);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        minioClient.putObject(
            PutObjectArgs.builder()
                .bucket(bucketName)
                .object(dir + "/" + filename.concat(suffix))
                .stream(inputStream, data.length, -1)
                .contentType("application/octet-stream")
                .build()
        );
    }

    private MinioTopicPartitionRemoteLogFinder safeGetFinder(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return topicFinderMap.computeIfAbsent(remoteLogSegmentMetadata.topicIdPartition(),
            tip -> new MinioTopicPartitionRemoteLogFinder(minioClient, tip, bucketName));
    }

}
