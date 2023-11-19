package com.bigdata.flink.aggregate;

import com.bigdata.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author itender
 * @date 2023/11/18/ 21:09
 * @desc
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensors = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 55),
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

        // keyBy之后才能聚合
        // 传位置只能适用于tuple
        // keyedStream.sum(2);
        // 直接穿字段名称
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                return new WaterSensor(waterSensor.getId(), waterSensor.getTs(), waterSensor.getVc() + t1.getVc());
            }
        });
        result.print();
        env.execute();
    }
}
