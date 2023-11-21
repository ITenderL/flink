package com.bigdata.flink.window;

import com.bigdata.flink.bean.WaterSensor;
import com.bigdata.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author itender
 * @date 2023/11/20/ 21:30
 * @desc
 */
public class WindowAggregateDemo {
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
        // aggregate累加器，
        // 第一个参数：输入类型
        // 第二个参数：累加器的类型，存储中间计算结果的类型
        // 第三个参数：返回值的类型呢
        SingleOutputStreamOperator<String> dataStream = sensorWs.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
            /**
             * 初始化累加器
             * @return
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器！");
                return 0;
            }

            /**
             * 聚合逻辑
             * @param waterSensor
             * @param accumulator
             * @return
             */
            @Override
            public Integer add(WaterSensor waterSensor, Integer accumulator) {
                System.out.println("调用add方法");
                return accumulator + waterSensor.getVc();
            }

            /**
             * 获取最终结果，输出
             * @param accumulator
             * @return
             */
            @Override
            public String getResult(Integer accumulator) {
                return accumulator.toString();
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                System.out.println("调用merge");
                return null;
            }
        });


        dataStream.print();
        env.execute();
    }
}
