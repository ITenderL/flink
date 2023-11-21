package com.bigdata.flink.window;

import com.bigdata.flink.bean.WaterSensor;
import com.bigdata.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 * @author cjp
 * @version 1.0
 */
public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("172.31.25.125", 7777)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 2. 窗口函数：
        /**
         * 增量聚合 Aggregate + 全窗口 process
         * 1、增量聚合函数处理数据： 来一条计算一条
         * 2、窗口触发时， 增量聚合的结果（只有一条） 传递给 全窗口函数
         * 3、经过全窗口函数的处理包装后，输出
         *
         * 结合两者的优点：
         * 1、增量聚合： 来一条计算一条，存储中间的计算结果，占用的空间少
         * 2、全窗口函数： 可以通过 上下文 实现灵活的功能
         */

//        sensorWS.reduce()   //也可以传两个

        SingleOutputStreamOperator<String> result = sensorWS.aggregate(
                new MyAgg(),
                new MyProcess()
        );

        result.print();



        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String>{

        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器");
            return 0;
        }


        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("调用add方法,value="+value);
            return accumulator + value.getVc();
        }

        @Override
        public String getResult(Integer accumulator) {
            System.out.println("调用getResult方法");
            return accumulator.toString();
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            System.out.println("调用merge方法");
            return null;
        }
    }

    // 全窗口函数的输入类型 = 增量聚合函数的输出类型
    public static class MyProcess extends ProcessWindowFunction<String,String,String,TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            long startTs = context.window().getStart();
            long endTs = context.window().getEnd();
            String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

            long count = elements.spliterator().estimateSize();

            out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());

        }
    }
}
