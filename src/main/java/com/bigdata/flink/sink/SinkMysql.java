package com.bigdata.flink.sink;

import com.bigdata.flink.bean.WaterSensor;
import com.bigdata.flink.functions.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author itender
 * @date 2023/11/20/ 13:19
 * @desc
 */
public class SinkMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 必须要开，如果是精准一次无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<WaterSensor> dataSource = env
                .socketTextStream("172.31.25.125", 7777)
                .map(new WaterSensorMapFunction());

        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink("insert into ws values(?, ?, ?)",
                (JdbcStatementBuilder<WaterSensor>) (preparedStatement, waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTs());
                    preparedStatement.setInt(3, waterSensor.getVc());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/demo_01?useSSL=false&autoReconnect=true&characterEncoding=utf8")
                        .withUsername("root")
                        .withPassword("root")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );
        dataSource.addSink(jdbcSink);
        env.execute();
    }
}
