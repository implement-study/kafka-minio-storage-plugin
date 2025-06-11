package com.shengsheng.kafka.storage.minio;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import java.text.NumberFormat;
import java.util.EnumMap;
import java.util.Map;

import static com.shengsheng.kafka.storage.minio.RemoteUtils.topicDirFromTopicIdPartition;

/**
 * Represents a group of MinioSegmentFiles corresponding to a single Kafka log segment.
 * Each group contains six MinioSegmentFiles (note: the transaction index file may not always exist).
 * This object allows direct operations, internally invoking the appropriate MinIO APIs.
 * <p>
 * The file names are the same as the Kafka log segment files.
 * <p>
 * Storage path pattern:
 * /${kafka-data-bucketName}/${topicName}-${partitionId}/0000000000.log
 * /0000000000.index
 * ...
 *
 * @author gongxuanzhangmelt@gmail.com
 **/
public class MinioSegmentFileset {

    private final Map<SegmentFileType, MinioSegmentFile> files;
    private final MinioClientWrapper client;
    private final long offset;
    private final TopicIdPartition topicIdPartition;


    private MinioSegmentFileset(MinioClientWrapper client, RemoteLogSegmentMetadata metadata) {
        this.files = new EnumMap<>(SegmentFileType.class);
        this.client = client;
        this.offset = metadata.startOffset();
        this.topicIdPartition = metadata.topicIdPartition();
        for (SegmentFileType type : SegmentFileType.values()) {
            files.put(type, new MinioSegmentFile(type, topicDirFromTopicIdPartition(topicIdPartition), offset));
        }
    }


    public static MinioSegmentFileset open(MinioClientWrapper client, RemoteLogSegmentMetadata metadata) {
        return new MinioSegmentFileset(client, metadata);
    }

    public void uploadToMinio() throws Exception {
        files.values().forEach(client::uploadFile);
    }

    public void removeFromMinio() throws Exception {

    }

    public enum SegmentFileType {

        LOG(".log"),
        OFFSET(".index"),
        TIMESTAMP(".timeindex"),
        TXN(".txnindex"),
        SNAPSHOT(".snapshot"),
        EPOCH(".leader-epoch-checkpoint");

        private final String suffix;

        SegmentFileType(String suffix) {
            this.suffix = suffix;
        }

        public String toFilename(long offset) {
            return filenamePrefixFromOffset(offset).concat(suffix);
        }

        private String filenamePrefixFromOffset(long offset) {
            NumberFormat nf = NumberFormat.getInstance();
            nf.setMinimumIntegerDigits(20);
            nf.setMaximumFractionDigits(0);
            nf.setGroupingUsed(false);
            return nf.format(offset);
        }
    }

}
