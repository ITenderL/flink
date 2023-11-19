package com.bigdata.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: ITender
 * @date: 2023/05/27/ 12:44
 * @desc:
 */
public class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String str, Collector<Tuple2<String, Integer>> collector) {
        String[] arr = str.split("\\s+");
        for (String s : arr) {
            collector.collect(new Tuple2<>(s, 1));
        }
    }
}
