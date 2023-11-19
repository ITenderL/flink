package com.bigdata.flink.partition;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: ITender
 * @date: 2023/05/27/ 15:45
 * @desc:
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<String> dataStream = env.socketTextStream("172.31.25.125", 7777);

        dataStream
                // 随机分区
                // .shuffle()
                // 轮询分区，如果是数据源倾斜的，source读进来以后，可以调用rebalance可以解决数据源的倾斜
                // .rebalance()
                // 缩放
                // .rescale()
                // 广播，分发到下游所有的子任务
                // .broadcast()
                // 全部请求发到第一个子任务
                .global()
                .print();
        env.execute("WordCountStreamUnbounded");
    }
}
