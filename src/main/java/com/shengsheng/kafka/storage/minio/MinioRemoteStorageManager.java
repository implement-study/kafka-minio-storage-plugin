package com.shengsheng.kafka.storage.minio;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import static com.shengsheng.kafka.storage.minio.MinioSegmentFileset.topicDir;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class MinioRemoteStorageManager implements RemoteStorageManager {

    private static final Logger LOG = LoggerFactory.getLogger(MinioRemoteStorageManager.class);

    public static final String MINIO_ENDPOINT_CONFIG_KEY = "minio.endpoint";
    public static final String MINIO_USERNAME_CONFIG_KEY = "minio.username";
    public static final String MINIO_PASSWORD_CONFIG_KEY = "minio.password";
    public static final String MINIO_KAFKA_BUCKET_CONFIG_KEY = "minio.kafka.bucket";

    private MinioClient minioClient;

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata metadata,
                                                                                LogSegmentData logSegmentData) throws RemoteStorageException {
        MinioSegmentFileset fileset = MinioSegmentFileset.open(minioClient, metadata, logSegmentData);
        try {
            fileset.uploadToMinio();
        } catch (Exception e) {
            try {
                fileset.removeFromMinio();
            } catch (Exception ex) {
                //  ignore remove exception
            }
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
        MinioSegmentFileset fileset = MinioSegmentFileset.open(minioClient, metadata);
        MinioSegmentFile log = fileset.getSegmentFile(MinioSegmentFileset.SegmentFileType.LOG);
        try {
            return log.fileStream(minioClient, startPosition);
        } catch (Exception e) {
            throw new RemoteStorageException(e);
        }
    }


    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata metadata, IndexType indexType) throws RemoteStorageException {
        long start = metadata.startOffset();
        long endOffset = metadata.endOffset();
        System.out.printf("fetch index [%s] start[%d] end[%d] %n", indexType, start, endOffset);

        MinioSegmentFileset fileset = MinioSegmentFileset.open(minioClient, metadata);
        MinioSegmentFile segmentFile = fileset.getSegmentFile(convertType(indexType));
        try {
            return segmentFile.fileStream(minioClient, metadata.startOffset());
        } catch (Exception e) {
            return InputStream.nullInputStream();
        }
    }

    // minio/minio:RELEASE.2025-04-08T15-41-24Z
    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        try {
            TopicIdPartition tip = metadata.topicIdPartition();
            System.out.printf("deleteLogSegmentData topic %s partition %d start [%d] end [%d] %n",
                tip.topic(), tip.partition(), metadata.startOffset(), metadata.endOffset());
            this.minioClient.removeDir(topicDir(tip), filename -> {
                String offsetName = filename.substring(0, filename.lastIndexOf("."));
                long fileOffset = Long.parseLong(offsetName);
                return fileOffset <= metadata.startOffset();
            });
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
        String endpoint = configs.get(MINIO_ENDPOINT_CONFIG_KEY).toString();
        String username = configs.get(MINIO_USERNAME_CONFIG_KEY).toString();
        String password = configs.get(MINIO_PASSWORD_CONFIG_KEY).toString();
        String bucketName = "kafka-data";
        if (configs.containsKey(MINIO_KAFKA_BUCKET_CONFIG_KEY)) {
            bucketName = configs.get(MINIO_KAFKA_BUCKET_CONFIG_KEY).toString();
        }
        this.minioClient = new MinioClient(io.minio.MinioClient.builder()
            .endpoint(endpoint)
            .credentials(username, password)
            .build(), bucketName);
        try {
            minioClient.createBucketIfNotExist(bucketName);
        } catch (Exception e) {
            throw new InvalidConfigurationException("init minio client error");
        }
    }

    private MinioSegmentFileset.SegmentFileType convertType(IndexType type) {
        return switch (type) {
            case OFFSET -> MinioSegmentFileset.SegmentFileType.OFFSET;
            case TIMESTAMP -> MinioSegmentFileset.SegmentFileType.TIMESTAMP;
            case PRODUCER_SNAPSHOT -> MinioSegmentFileset.SegmentFileType.SNAPSHOT;
            case TRANSACTION -> MinioSegmentFileset.SegmentFileType.TXN;
            case LEADER_EPOCH -> MinioSegmentFileset.SegmentFileType.EPOCH;
        };
    }


}
