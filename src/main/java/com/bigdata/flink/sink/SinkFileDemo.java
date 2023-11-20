package com.bigdata.flink.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @author itender
 * @date 2023/11/19/ 15:59
 * @desc
 */
public class SinkFileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一共四个参数，第一个参数GeneratorFunction需要实现接口，重写map方法，输入固定类型是long，
        // 第二个参数：Long类型自动生成数字的最大值，默认从1开始，达到这个值就停止了，
        // 第三个参数：限速策略，比如每秒生成几条数据，
        // 第四个参数：返回值类型
        env.setParallelism(1);

        // 必须开启checkpoint否则文件一直都是inprocessing
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                (Long value) -> "Number:" + value,
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );
        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource");
        FileSink<String> fileSink = FileSink
                .<String>forRowFormat(new Path("f:/tmp"), new SimpleStringEncoder<>())
                .withOutputFileConfig(
                        OutputFileConfig
                                .builder()
                                .withPartPrefix("itender-")
                                .withPartSuffix(".txt")
                                .build()
                )
                // 按照目录分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 文件滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withInactivityInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(1024 * 1024)
                                .build()
                )
                .build();
        // 输出到文件系统
        dataGen.sinkTo(fileSink);
        env.execute();
    }
}
