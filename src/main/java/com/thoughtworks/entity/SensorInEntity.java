package com.thoughtworks.entity;

import com.thoughtworks.enums.MetricTypeEnum;
import com.thoughtworks.util.StringUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: spring du
 * @description: 传感器输入实体
 * @date: 2021/1/6 17:01
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorInEntity extends BaseEntity{

    // 传感器类型 T1-表示温度传感器 Q1-表示油品质量检测传感器
    private String sensorType;
    // 传感器时间
    private String sensorTime;
    // 传感器采集值
    private String value;

    public SensorInEntity(String data) {
        String newData = StringUtils.splitEndChar(data);
        String sensorFlag = newData.split(",")[0];
        String[] values = newData.split(",");
        sensorType = sensorFlag;
        sensorTime = values[1];
        if (MetricTypeEnum.T.getCode().equals(sensorFlag)) { // T1
            value = values[2];
        } else { // Q1
            value = values[2] + "," +values[3] + "," + values[4];
        }
    }

    @Override
    public String toString() {
        return sensorType + "," + sensorTime + "," + value;
    }
}
