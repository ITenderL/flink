package com.bigdata.flink.split;

import com.bigdata.flink.bean.WaterSensor;
import com.bigdata.flink.functions.MyMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author itender
 * @date 2023/11/19/ 14:30
 * @desc
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        SingleOutputStreamOperator<WaterSensor> map = env
                .socketTextStream("172.31.25.125", 7777)
                .map(new MyMapFunction());
        OutputTag<WaterSensor> outputTags1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> outputTags2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> process = map.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                String id = waterSensor.getId();
                if ("s1".equals(id)) {
                    // OutputTag<WaterSensor> outputTags1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
                    // 调用context.output()将数据放入侧输出流，第一个参数是outputTag，第二个参数是要输出的数据
                    context.output(outputTags1, waterSensor);
                } else if ("s2".equals(id)) {
                    // OutputTag<WaterSensor> outputTags2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
                    context.output(outputTags2, waterSensor);
                } else {
                    // 非s1，s2的数据放入主输出流中
                    collector.collect(waterSensor);
                }
            }
        });
        // 打印主流中的数据
        process.print("主流数据：");
        // 获取s1侧输出流
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(outputTags1);
        // 获取s2侧输出流
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(outputTags2);
        s1.print("s1:");
        s2.print("s2:");
        env.execute();
    }
}
