package com.bigdata.flink.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author: ITender
 * @date: 2023/05/27/ 12:37
 * @desc:
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.读取数据，从文件中读取
        DataSet<String> dataSet = env.readTextFile("input/word.txt");
        // 3.切分，转换
        // 4.按照word分组
        // 5.各组内聚合
        DataSet<Tuple2<String, Integer>> result = dataSet.flatMap(new MyFlatMapper()).groupBy(0).sum(1);
        // 6.输出
        result.print();
        // env.execute("wordCount");
    }
}
