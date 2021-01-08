package com.thoughtworks.business.service;

/**
 * @author: spring du
 * @description: IOT数据处理业务接口定义
 * @date: 2021/1/7 13:49
 */
public interface IOTService {

    /**
     * 读取（输入）数据
     * @return
     */
    void readIOTData(String source);

    /**
     * 处理数据
     * @return
     */
    void dealIOTData() throws Exception;
}
