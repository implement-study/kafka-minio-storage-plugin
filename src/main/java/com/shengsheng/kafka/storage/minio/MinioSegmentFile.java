package com.shengsheng.kafka.storage.minio;


import java.io.InputStream;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public abstract class MinioSegmentFile implements MinioSource {

    private final MinioSegmentFileset.SegmentFileType type;

    private final String dir;

    private final long offset;

    protected MinioSegmentFile(MinioSegmentFileset.SegmentFileType type, String dir, long offset) {
        this.type = type;
        this.dir = dir;
        this.offset = offset;
    }

    public String filename() {
        return type.toFilename(offset);
    }

    public String objectName() {
        return dir + "/" + filename();
    }
    
    @Override
    public void remove(MinioClient client) throws Exception {
        client.removeObject(this.objectName());
    }

    @Override
    public InputStream fileStream(MinioClient client, long startPosition) throws Exception {
        return client.objectStream(this.objectName(), startPosition);
    }

}
