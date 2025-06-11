package com.shengsheng.kafka.storage.minio;


import java.nio.file.Path;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class PathSegmentFile extends MinioSegmentFile {

    private final Path path;

    public PathSegmentFile(MinioSegmentFileset.SegmentFileType type, String dir, long offset, Path path) {
        super(type, dir, offset);
        this.path = path;
    }

    @Override
    public void upload(MinioClientWrapper client) throws Exception {
        if (path != null) {
            client.uploadLocalPath(this.objectName(), path);
        }
    }

}
