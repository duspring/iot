package com.thoughtworks;

import com.thoughtworks.business.service.IOTService;
import com.thoughtworks.business.service.IOTServiceImpl;
import com.thoughtworks.constant.Constant;

/**
 * @author: spring du
 * @description: IOT数据分析处理入口
 * @date: 2021/1/8 16:34
 */
public class IOTMain {
    public static void main(String[] args) throws Exception {
        // 业务接口
        IOTService iotService = new IOTServiceImpl();

        // 读取数据
        iotService.readIOTData(Constant.INPUT_FILE_NAME);

        // 处理数据
        iotService.dealIOTData();
    }
}
