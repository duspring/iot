package com.thoughtworks.business.service;

import com.thoughtworks.business.function.ComputeAverageWithListState;
import com.thoughtworks.business.function.MyKeySelector;
import com.thoughtworks.business.function.MyMapFunction;
import com.thoughtworks.entity.SensorInEntity;
import com.thoughtworks.util.FileUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author: spring du
 * @description: IOT数据处理业务实现类
 * @date: 2021/1/7 13:55
 */
public class IOTServiceImpl implements IOTService {

    @Override
    public void readIOTData(String fileName) {
        // 读取输入数据
        List<String> inputData = FileUtils.readFile(fileName);
        // 显示输入内容数据
        inputDisplay(inputData);
    }

    @Override
    public void dealIOTData() throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 从文件读取数据、转换成 bean对象
        SingleOutputStreamOperator<SensorInEntity> sensorDS = env
                .readTextFile(FileUtils.getResourcePath())
                .map(new MyMapFunction());
        // 分组聚合并处理
        sensorDS.keyBy(new MyKeySelector())
                .flatMap(new ComputeAverageWithListState(3,5))
                .print();

        // 执行
        env.execute();
    }


    /**
     * 显示输入内容
     * @param inputs
     */
    private void inputDisplay(List<String> inputs) {
        System.out.println("输入:");
        inputs.forEach(line -> System.out.println(line));
        System.out.println();
        System.out.println("数据正在处理中...");
        System.out.println();
    }
}
