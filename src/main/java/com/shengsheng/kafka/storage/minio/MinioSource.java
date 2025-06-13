package com.shengsheng.kafka.storage.minio;

import java.io.InputStream;

/**
 * An abstraction of a MinIO resource, operable directly via MinioClient.
 *
 * @author gongxuanzhangmelt@gmail.com
 **/
public interface MinioSource {

    /**
     * upload this source to minio
     **/
    void upload(MinioClient client) throws Exception;

    /**
     * remove this source from minio if exists
     **/
    void remove(MinioClient client) throws Exception;


    InputStream fileStream(MinioClient client, long startPosition) throws Exception;

}
