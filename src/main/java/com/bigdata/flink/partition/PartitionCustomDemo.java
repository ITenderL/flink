package com.bigdata.flink.partition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author itender
 * @date 2023/11/19/ 14:18
 * @desc
 */
public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<String> dataStream = env.socketTextStream("172.31.25.125", 7777);

        dataStream.partitionCustom(new MyPartitioner(), key -> key).print();
        env.execute();
    }
}
