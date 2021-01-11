package com.thoughtworks.business.service;

import com.thoughtworks.business.function.ComputeAverageWithListState;
import com.thoughtworks.business.function.MyKeySelector;
import com.thoughtworks.business.function.MyMapFunction;
import com.thoughtworks.business.function.RichCoFlatMapAverageWithListState;
import com.thoughtworks.entity.SensorInEntity;
import com.thoughtworks.util.FileUtils;
import com.thoughtworks.util.StringUtils;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author springdu
 * @create 2021/1/11 21:16
 * @description TODO
 */
public class IOTPreServiceImpl extends BaseIOTDao implements IOTService {
    @Override
    public void readIOTData(String fileName) {
        readIOTInputData(fileName);
    }

    @Override
    public void dealIOTData() throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 获取数据源
        List<String> inputs = FileUtils.readFile("ioT.txt");
        List<SensorInEntity> sensorTList = new ArrayList<>();
        List<SensorInEntity> sensorQList = new ArrayList<>();
        inputs.forEach(line -> {
            String newData = StringUtils.splitEndChar(line);
            String sensorFlag = newData.split(",")[0];
            String[] values = newData.split(",");
            if (line.startsWith("T1")) {
                sensorTList.add(new SensorInEntity(sensorFlag, values[1], values[2]));
            } else {
                sensorQList.add(new SensorInEntity(sensorFlag,values[1] , values[2] + "," +values[3] + "," + values[4]));
            }
        });

        // 从文件读取T数据、转换成 bean对象
        DataStreamSource<SensorInEntity> dataStreamTSource = env.fromCollection(sensorTList);

        // 从文件读取Q数据、转换成 bean对象
        DataStreamSource<SensorInEntity> dataStreamQSource = env.fromCollection(sensorQList);

        // 将T和Q数据连接在一起
        ConnectedStreams<SensorInEntity, SensorInEntity> connectDataStreamSource = dataStreamTSource.connect(dataStreamQSource);

        // 分组聚合 相同key放在一起处理
        ConnectedStreams<SensorInEntity, SensorInEntity> keyByConnectDataStreamSource =
                connectDataStreamSource.keyBy(new MyKeySelector(), new MyKeySelector());

        // 分组聚合并处理
        keyByConnectDataStreamSource.flatMap(new RichCoFlatMapAverageWithListState()).print();

        // 执行
        env.execute();
    }
}
