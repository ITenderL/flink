package com.bigdata.flink.transform;

import com.bigdata.flink.bean.WaterSensor;
import com.bigdata.flink.functions.MyMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author itender
 * @date 2023/11/18/ 21:09
 * @desc
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensors = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s4", 4L, 4)
        );
        // 算子：一进一出

        // 第一种方式：匿名内部类
        // SingleOutputStreamOperator<String> map = sensors.map(new MapFunction<WaterSensor, String>() {
        //     @Override
        //     public String map(WaterSensor waterSensor) throws Exception {
        //         return waterSensor.getId();
        //     }
        // });
        // 方式二：lambda表达式
        // SingleOutputStreamOperator<String> map = sensors.map(WaterSensor::getId);

        // 第三种方式：自定义实现类
        // SingleOutputStreamOperator<String> map = sensors.map(new MyMapFunction());
        // map.print();
        env.execute();
    }
}
