package com.thoughtworks.business.function;

import com.thoughtworks.entity.SensorInEntity;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author: spring du
 * @description: 自定义Map函数
 * @date: 2021/1/8 14:22
 */
public class MyMapFunction implements MapFunction<String, SensorInEntity> {
    @Override
    public SensorInEntity map(String value) throws Exception {
        return new SensorInEntity(value);
    }
}
