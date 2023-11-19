package com.bigdata.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author itender
 * @date 2023/11/18/ 20:44
 * @desc
 */
public class DataGenerateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一共四个参数，第一个参数GeneratorFunction需要实现接口，重写map方法，输入固定类型是long，
        // 第二个参数：Long类型自动生成数字的最大值，默认从1开始，达到这个值就停止了，
        // 第三个参数：限速策略，比如每秒生成几条数据，
        // 第四个参数：返回值类型
        env.setParallelism(1);
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                (Long value) -> "Number:" + value,
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource").print();
        env.execute();
    }
}
