package com.bigdata.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @author itender
 * @date 2023/11/19/ 21:57
 * @desc
 */
public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 必须要开，如果是精准一次无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<String> streamSource = env
                .socketTextStream("172.31.25.125", 7777);
        // .map(new WaterSensorMapFunction());

        // kafkaSink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("172.31.25.125:9092")
                // 序列化器
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("topic-test")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 写到kafka一致性级别，精准一次，至少一次
                // .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                // 精准一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置事务前置
                .setTransactionalIdPrefix("itender-")
                // 如果是精准一次，需要设置事务的超时时间，大于checkpoint时间，小于15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();
        streamSource.sinkTo(kafkaSink);
        env.execute();
    }
}
