package com.thoughtworks.enums;

/**
 * @author: spring du
 * @description: 枚举列表
 * @date: 2021/1/6 16:59
 */
public enum MetricTypeEnum {

    AB("AB", "酸度"),
    AE("AE", "粘稠度"),
    CE("CE", "含水量"),
    FIRST("FIRST","第一次"),
    SECOND("SECOND", "第二次"),
    HIGH("HIGH", "过高"),
    T("T1", "温度传感器"),
    Q("Q1", "油品质量检测传感器");

    private String code;
    private String desc;

    MetricTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
