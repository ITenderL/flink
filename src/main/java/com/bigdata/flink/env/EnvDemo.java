package com.bigdata.flink.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author itender
 * @date 2023/11/18/ 15:55
 * @desc
 */
public class EnvDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //指定 Flink Web UI 端口为9091
        configuration.set(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env
                // 设置批处理方式，可以设置为批处理  也可以设置为流式处理
                .setRuntimeMode(RuntimeExecutionMode.BATCH)
                .socketTextStream("172.31.25.125", 7777)
                .flatMap((String str, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] arr = str.split("\\s+");
                    for (String s : arr) {
                        collector.collect(new Tuple2<>(s, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();
        env.execute();
    }
}
