package com.bigdata.flink.functions;

import com.bigdata.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class FilterFunctionImpl implements FilterFunction<WaterSensor> {

    public String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return this.id.equals(value.getId());
    }
}
