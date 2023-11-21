package com.bigdata.flink.watermark;

import com.bigdata.flink.bean.WaterSensor;
import com.bigdata.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author itender
 * @date 2023/11/20/ 21:30
 * @desc
 */
public class WaterMarkMonoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<WaterSensor> sensorDs = env
                .socketTextStream("172.31.25.125", 7777)
                .map(new WaterSensorMapFunction());
        // 指定watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 升序的watermark,没有等待时间
                .<WaterSensor>forMonotonousTimestamps()
                // 指定时间戳分配器，从数据中提取
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        System.out.println("数据 = " + waterSensor + ", recordTs = " + l);
                        return waterSensor.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> sensorDsWithWaterMark = sensorDs.assignTimestampsAndWatermarks(watermarkStrategy);

        sensorDsWithWaterMark.keyBy(WaterSensor::getId)
                // 使用事件语义的窗口，
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            /**
                             * 全窗口函数计算逻辑：  窗口触发时才会调用一次，统一计算窗口的所有数据
                             * @param s   分组的key
                             * @param context  上下文
                             * @param elements 存的数据
                             * @param out      采集器
                             * @throws Exception
                             */
                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                // 上下文可以拿到window对象，还有其他东西：侧输出流 等等
                                long startTs = context.window().getStart();
                                long endTs = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                long count = elements.spliterator().estimateSize();
                                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]包含" + count + "条数据===>" + elements.toString());
                            }
                        }
                )
                .print();
        env.execute();
    }
}
