package com.bigdata.flink.transform;

import com.bigdata.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author itender
 * @date 2023/11/18/ 21:09
 * @desc
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensors = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s4", 4L, 4)
        );
        // flatMap：一进多出，调用几次collector就输出几条
        SingleOutputStreamOperator<String> flatMap = sensors.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
                // 如果是s1，输出vc，如果是s2分别输出ts和vc
                if ("s1".equals(waterSensor.getId())) {
                    collector.collect(waterSensor.getVc().toString());
                } else if("s2".equals(waterSensor.getId())) {
                    collector.collect(waterSensor.getTs().toString());
                    collector.collect(waterSensor.getVc().toString());
                }
            }
        });
        flatMap.print();
        env.execute();
    }
}
