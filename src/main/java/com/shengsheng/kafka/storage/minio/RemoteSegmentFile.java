package com.shengsheng.kafka.storage.minio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class RemoteSegmentFile extends MinioSegmentFile {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteSegmentFile.class);

    public RemoteSegmentFile(MinioSegmentFileset.SegmentFileType type, String dir, long offset) {
        super(type, dir, offset);
    }

    @Override
    public void upload(MinioClient client) throws Exception {
        LOG.warn("remote file can't upload to minio object :[{}]", this.objectName());
    }
}
