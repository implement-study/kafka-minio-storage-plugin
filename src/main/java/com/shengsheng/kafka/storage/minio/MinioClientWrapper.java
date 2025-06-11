package com.shengsheng.kafka.storage.minio;

import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.UploadObjectArgs;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class MinioClientWrapper implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MinioClientWrapper.class);

    private final MinioClient client;

    private final String kafkaDataBucket;

    public MinioClientWrapper(MinioClient minioClient, String kafkaDataBucket) {
        this.client = minioClient;
        this.kafkaDataBucket = kafkaDataBucket;
    }


    public void uploadLocalPath(String objectName, Path path) throws Exception {
        client.uploadObject(
            UploadObjectArgs.builder()
                .bucket(kafkaDataBucket)
                .object(objectName)
                .filename(path.toAbsolutePath().toString())
                .build());
    }

    public void uploadByteBuffer(String objectName, ByteBuffer buffer) throws Exception {
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        client.putObject(
            PutObjectArgs.builder()
                .bucket(kafkaDataBucket)
                .object(objectName)
                .stream(inputStream, data.length, -1)
                .contentType("application/octet-stream")
                .build());
    }

    public List<String> listDirFilename(String dir) throws Exception {
        return listDirFilename(dir, filename -> true);
    }

    /**
     * return filename without dir prefix
     **/
    public List<String> listDirFilename(String dir, Predicate<String> filenameFilter) throws Exception {
        Iterable<Result<Item>> allObjects = client.listObjects(
            ListObjectsArgs.builder()
                .bucket(kafkaDataBucket)
                .prefix(dir + "/")
                .build());
        List<String> result = new ArrayList<>();
        for (Result<Item> resultItem : allObjects) {
            Item item = resultItem.get();
            String objectName = item.objectName();
            String filename = objectName.substring(dir.length() + 1);
            if (filenameFilter.test(filename)) {
                result.add(filename);
            }
        }
        return result;
    }

    public InputStream objectStream(String ob, long startPosition) throws Exception {
        return client.getObject(
            GetObjectArgs.builder()
                .bucket(kafkaDataBucket)
                .object(ob)
                .offset(startPosition)
                .build());
    }


    public void removeDir(String dir, Predicate<String> filenameFilter) throws Exception {
        Iterable<Result<Item>> results = client.listObjects(
            ListObjectsArgs.builder()
                .bucket(kafkaDataBucket)
                .prefix(dir + "/")
                .build());
        Map<String, DeleteObject> deleteObjects = new HashMap<>();
        for (Result<Item> result : results) {
            String objectName = result.get().objectName();
            String filename = objectName.substring(dir.length() + 1);
            if (filenameFilter.test(filename)) {
                deleteObjects.put(objectName, new DeleteObject(objectName));
            }
        }
        if (deleteObjects.isEmpty()) {
            return;
        }
        client.removeObjects(
            RemoveObjectsArgs.builder()
                .bucket(kafkaDataBucket)
                .objects(deleteObjects.values())
                .build());
        LOG.info("delete [{}] from minio dir[{}]", deleteObjects.keySet(), dir);
    }

    public void removeObject(String objectName) throws Exception {
        client.removeObject(
            io.minio.RemoveObjectArgs.builder()
                .bucket(kafkaDataBucket)
                .object(objectName)
                .build());
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }

}
