package com.bigdata.flink.aggregate;

import com.bigdata.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author itender
 * @date 2023/11/18/ 21:09
 * @desc
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensors = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s4", 4L, 4)
        );
        // 按照id来分组，
        // 1、返回的是一个keyedStream键控流
        // 2、keyBy不是转换算子，只是对数据进行重分区不能设置并行度
        // 3、分组 与 分区的取别  分组：相同key的数据在同意分区   分区：一个子任务可以认为是一个分区
        KeyedStream<WaterSensor, String> keyedStream = sensors.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });
        env.execute();
    }
}
