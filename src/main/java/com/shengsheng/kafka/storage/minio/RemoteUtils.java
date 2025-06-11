package com.shengsheng.kafka.storage.minio;

import org.apache.kafka.common.TopicIdPartition;

import java.text.NumberFormat;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class RemoteUtils {

    private RemoteUtils() {

    }


    public static String topicDirFromTopicIdPartition(TopicIdPartition topicIdPartition) {
        return topicIdPartition.topic() + "-" + topicIdPartition.partition();
    }

    public static String filenamePrefixFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }


}
