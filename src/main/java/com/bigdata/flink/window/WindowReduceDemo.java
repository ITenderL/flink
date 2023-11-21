package com.bigdata.flink.window;

import com.bigdata.flink.bean.WaterSensor;
import com.bigdata.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author itender
 * @date 2023/11/20/ 21:30
 * @desc
 */
public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<WaterSensor> sensorDs = env
                .socketTextStream("172.31.25.125", 7777)
                .map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKs = sensorDs.keyBy(WaterSensor::getId);

        // 1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWs = sensorKs.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        // 2.窗口函数 reduce函数
        SingleOutputStreamOperator<WaterSensor> reduce = sensorWs.reduce((ReduceFunction<WaterSensor>) (waterSensor, t1) -> {
            System.out.println("调用reduce方法,value1:" + waterSensor + "value2: " + t1);
            return new WaterSensor(waterSensor.getId(), t1.getTs(), waterSensor.getVc() + t1.getVc());
        });

        // 2.窗口函数 reduce函数
        // SingleOutputStreamOperator<WaterSensor> reduce = sensorWs.reduce(new ReduceFunction<WaterSensor>() {
        //     @Override
        //     public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
        //         System.out.println("调用reduce方法,value1:" + waterSensor + "value2: " + t1);
        //         return new WaterSensor(waterSensor.getId(), t1.getTs(), waterSensor.getVc() + t1.getVc());
        //     }
        // });
        reduce.print();
        env.execute();
    }
}
