package com.bigdata.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: ITender
 * @date: 2023/05/27/ 15:45
 * @desc:
 */
public class WordCountStreamUnbounded {
    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        //指定 Flink Web UI 端口为9091
        configuration.setString("rest.port","9091");
        // 并行度优先级  算子 > env > 提交任务时指定 > 配置文件
        // IDEA运行时也能看见webUI，一般用于本地测试
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setParallelism(2);
        DataStream<String> dataStream = env.socketTextStream("172.31.25.125", 7777);
        DataStream<Tuple2<String, Integer>> result = dataStream
                .flatMap((String str, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] arr = str.split("\\s+");
                    for (String s : arr) {
                        collector.collect(new Tuple2<>(s, 1));
                    }
                })
                // 算子指定
                // .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)
                .sum(1);
        result.print();
        env.execute("WordCountStreamUnbounded");
    }
}
