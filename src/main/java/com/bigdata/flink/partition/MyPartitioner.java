package com.bigdata.flink.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author itender
 * @date 2023/11/19/ 14:22
 * @desc
 */
public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int number) {
        return Integer.parseInt(key) % number;
    }
}
