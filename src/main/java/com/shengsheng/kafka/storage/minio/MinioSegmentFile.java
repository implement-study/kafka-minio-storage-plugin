package com.shengsheng.kafka.storage.minio;


/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class MinioSegmentFile {

    private final MinioSegmentFileset.SegmentFileType type;

    private final String dir;

    private final long offset;

    public MinioSegmentFile(MinioSegmentFileset.SegmentFileType type, String dir, long offset) {
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

}
