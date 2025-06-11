package com.shengsheng.kafka.storage.minio;


import java.nio.ByteBuffer;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class ByteBufferSegmentFile extends MinioSegmentFile {

    private final ByteBuffer byteBuffer;

    public ByteBufferSegmentFile(MinioSegmentFileset.SegmentFileType type, String dir, long offset,
                                 ByteBuffer byteBuffer) {
        super(type, dir, offset);
        this.byteBuffer = byteBuffer;
    }

    @Override
    public void upload(MinioClientWrapper client) throws Exception {
        client.uploadByteBuffer(this.objectName(), this.byteBuffer);
    }
   
}
