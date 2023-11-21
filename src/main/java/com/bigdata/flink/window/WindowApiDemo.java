package com.bigdata.flink.window;

import com.bigdata.flink.bean.WaterSensor;
import com.bigdata.flink.functions.WaterSensorMapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author itender
 * @date 2023/11/20/ 21:30
 * @desc
 */
public class WindowApiDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 必须要开，如果是精准一次无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<WaterSensor> sensorDs = env
                .socketTextStream("172.31.25.125", 7777)
                .map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKs = sensorDs.keyBy(WaterSensor::getId);

        // 1.指定窗口分配器
        // 1.1 没有keyBy的窗口，所有的数据进入同一个子任务，并行度只能为1
        // sensorDs.windowAll();

        // 1.2 keyby的窗口：每个key都定义了一组窗口，各自独立的进行统计计算
        // 1.2.1基于时间的，滚动窗口，窗口长度为10s
        // sensorKs.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        // 基于时间的，滑动窗口，窗口长度为10s, 滑动步长为2s
        // sensorKs.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)));
        // 会话窗口，超时时间为5s
        sensorKs.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        // 1.2.2 基于计数的
        // 滚动窗口，窗口长度5个元素
        sensorKs.countWindow(5);
        // 滑动窗口，窗口长度5个元素，滑动步长2个元素
        sensorKs.countWindow(5, 2);
        // 全局窗口，需要自定义触发器
        sensorKs.window(GlobalWindows.create());

        // 2.指定窗口函数  窗口内的数据，计算逻辑
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = sensorKs.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 增量聚合：来一条数据处理一条数据，窗口触发的时间会输出计算结果

        // 全窗口聚合：数据来了不计算，存起来，窗口触发在计算并输出结果

    }
}
