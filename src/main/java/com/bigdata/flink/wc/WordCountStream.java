package com.bigdata.flink.wc;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: ITender
 * @date: 2023/05/27/ 12:54
 * @desc:
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readTextFile("input/word.txt");
        // DataStream<Tuple2<String, Integer>> resultDataStream = dataStream.flatMap(new MyFlatMapper()).keyBy(0).sum(1);
        DataStream<Tuple2<String, Integer>> resultDataStream = dataStream
                .flatMap(new MyFlatMapper())
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)
                .sum(1);
        resultDataStream.print();
        env.execute("WordCountStream");
    }
}
