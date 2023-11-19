package com.bigdata.flink.split;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author itender
 * @date 2023/11/19/ 14:30
 * @desc
 */
public class SplitFilterDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<String> dataStream = env.socketTextStream("172.31.25.125", 7777);

        dataStream.filter(value -> Integer.parseInt(value) % 2 == 0).print("偶数流：");
        dataStream.filter(value -> Integer.parseInt(value) % 2 == 1).print("奇数流：");
        env.execute();
    }
}
