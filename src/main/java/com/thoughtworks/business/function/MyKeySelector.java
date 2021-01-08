package com.thoughtworks.business.function;

import com.thoughtworks.entity.SensorInEntity;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author: spring du
 * @description: 自定义分组的Key函数
 * @date: 2021/1/6 18:03
 */
public class MyKeySelector implements KeySelector<SensorInEntity, String> {
    @Override
    public String getKey(SensorInEntity value) throws Exception {
        return value.getSensorType();
    }
}
