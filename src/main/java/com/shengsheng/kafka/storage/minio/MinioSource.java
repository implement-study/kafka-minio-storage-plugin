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
    void upload(MinioClientWrapper client) throws Exception;

    /**
     * remove this source from minio if exists
     **/
    void remove(MinioClientWrapper client) throws Exception;

    
    InputStream download(MinioClientWrapper client, int startPosition) throws Exception;
}
