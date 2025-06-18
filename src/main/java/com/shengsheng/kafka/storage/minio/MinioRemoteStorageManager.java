package com.shengsheng.kafka.storage.minio;

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
import java.util.Objects;
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
        LOG.info("fetchLogSegment start[{}]", metadata.startOffset());
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
        LOG.info("fetch index [{}] start[{}] end[{}]", indexType, start, endOffset);

        MinioSegmentFileset fileset = MinioSegmentFileset.open(minioClient, metadata);
        MinioSegmentFile segmentFile = fileset.getSegmentFile(convertType(indexType));
        try {
            return segmentFile.fileStream(minioClient, metadata.startOffset());
        } catch (Exception e) {
            return InputStream.nullInputStream();
        }
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        try {
            TopicIdPartition tip = metadata.topicIdPartition();
            LOG.info("deleteLogSegmentData topic {} partition {} start [{}] end [{}] ", tip.topic(), tip.partition(),
                metadata.startOffset(), metadata.endOffset());
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
            if (minioClient != null) {
                minioClient.close();
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        String endpoint = getRequireConfig(configs, MINIO_ENDPOINT_CONFIG_KEY);
        String username = getRequireConfig(configs, MINIO_USERNAME_CONFIG_KEY);
        String password = getRequireConfig(configs, MINIO_PASSWORD_CONFIG_KEY);
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

    public String getRequireConfig(Map<String, ?> configs, String key) {
        Object value = configs.get(key);
        Objects.requireNonNull(configs.get(MINIO_ENDPOINT_CONFIG_KEY), "minio config '" + key + "' can not be null");
        return value.toString();
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
