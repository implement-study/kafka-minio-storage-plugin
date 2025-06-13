package com.shengsheng.kafka.storage.minio;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.shengsheng.kafka.storage.minio.MinioSegmentFileset.SegmentFileType.EPOCH;
import static com.shengsheng.kafka.storage.minio.MinioSegmentFileset.SegmentFileType.LOG;
import static com.shengsheng.kafka.storage.minio.MinioSegmentFileset.SegmentFileType.OFFSET;
import static com.shengsheng.kafka.storage.minio.MinioSegmentFileset.SegmentFileType.SNAPSHOT;
import static com.shengsheng.kafka.storage.minio.MinioSegmentFileset.SegmentFileType.TIMESTAMP;
import static com.shengsheng.kafka.storage.minio.MinioSegmentFileset.SegmentFileType.TXN;

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

    private final MinioClient client;


    private MinioSegmentFileset(MinioClient client) {
        this.files = new EnumMap<>(SegmentFileType.class);
        this.client = client;
    }


    /**
     * open fileset with {@link LogSegmentData} use for copy.
     **/
    public static MinioSegmentFileset open(MinioClient client, RemoteLogSegmentMetadata metadata,
                                           LogSegmentData data) {
        MinioSegmentFileset fileset = new MinioSegmentFileset(client);
        String dir = topicDir(metadata.topicIdPartition());
        long offset = metadata.startOffset();
        fileset.files.put(LOG, new PathSegmentFile(LOG, dir, offset, data.logSegment()));
        fileset.files.put(OFFSET, new PathSegmentFile(OFFSET, dir, offset, data.offsetIndex()));
        fileset.files.put(TIMESTAMP, new PathSegmentFile(TIMESTAMP, dir, offset, data.timeIndex()));
        fileset.files.put(TXN, new PathSegmentFile(TXN, dir, offset, data.transactionIndex().orElse(null)));
        fileset.files.put(SNAPSHOT, new PathSegmentFile(SNAPSHOT, dir, offset, data.producerSnapshotIndex()));
        fileset.files.put(EPOCH, new ByteBufferSegmentFile(EPOCH, dir, offset, data.leaderEpochIndex()));
        return fileset;
    }


    /**
     * open fileset without {@link LogSegmentData} use for fetch.
     **/
    public static MinioSegmentFileset open(MinioClient client, RemoteLogSegmentMetadata metadata) {
        MinioSegmentFileset fileset = new MinioSegmentFileset(client);
        String dir = topicDir(metadata.topicIdPartition());
        for (SegmentFileType fileType : SegmentFileType.values()) {
            fileset.files.put(fileType, new RemoteSegmentFile(fileType, dir, metadata.startOffset()));
        }
        return fileset;
    }

    public void uploadToMinio() throws Exception {
        for (MinioSegmentFile file : files.values()) {
            file.upload(client);
        }
    }

    public void removeFromMinio() throws Exception {
        List<Exception> foreachExceptions = new ArrayList<>();
        for (MinioSegmentFile file : files.values()) {
            try {
                file.remove(client);
            } catch (Exception e) {
                foreachExceptions.add(e);
            }
        }
        if (!foreachExceptions.isEmpty()) {
            Exception main = new Exception("Errors occurred during processing");
            for (Throwable suppressed : foreachExceptions) {
                main.addSuppressed(suppressed);
            }
            throw main;
        }
    }

    public MinioSegmentFile getSegmentFile(SegmentFileType type) {
        return this.files.get(type);
    }

    public static String topicDir(TopicIdPartition topicIdPartition) {
        return topicIdPartition.topic() + "-" + topicIdPartition.partition();
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
