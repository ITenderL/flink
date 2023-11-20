package com.bigdata.flink.functions;

import com.bigdata.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 * @author cjp
 * @version 1.0
 */
public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
