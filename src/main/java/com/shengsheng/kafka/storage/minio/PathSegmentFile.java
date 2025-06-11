package com.shengsheng.kafka.storage.minio;


import java.nio.file.Path;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class PathSegmentFile extends MinioSegmentFile {

    private final Path path;

    public PathSegmentFile(MinioSegmentFileset.SegmentFileType type, String dir, long offset , Path path) {
        super(type, dir, offset);
        this.path = path;
    }

    @Override
    public void upload(MinioClientWrapper client) throws Exception {
        client.uploadLocalPath(this.objectName(), path);
    }

    @Override
    public void remove(MinioClientWrapper client) {

    }
}
