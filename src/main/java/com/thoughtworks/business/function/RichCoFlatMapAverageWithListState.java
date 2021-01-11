package com.thoughtworks.business.function;

import com.thoughtworks.entity.SensorInEntity;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author springdu
 * @create 2021/1/11 21:37
 * @description TODO
 */
public class RichCoFlatMapAverageWithListState extends RichCoFlatMapFunction<SensorInEntity, SensorInEntity, String> {

    /**
     * ListState ： 里面可以存很多数据
     */
    private ListState<SensorInEntity> listTState;

    //
    private ListState<SensorInEntity> listQState;

    //
    private String avgValue;


    /**
     * 初始化的方法，只会执行一次
     * 用来注册状态
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        // 注册T状态
        ListStateDescriptor<SensorInEntity> descriptorT =
                new ListStateDescriptor<>(
                        "sensorT", // 状态的名字
                        TypeInformation.of(SensorInEntity.class)); // 状态存储的数据类型

        listTState = getRuntimeContext().getListState(descriptorT);

        // 注册Q状态
        ListStateDescriptor<SensorInEntity> descriptorQ =
                new ListStateDescriptor<>(
                        "sensorQ", // 状态的名字
                        TypeInformation.of(SensorInEntity.class)); // 状态存储的数据类型

        listQState = getRuntimeContext().getListState(descriptorQ);
    }

    /**
     * 处理T传感器数据
     * @param element
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap1(SensorInEntity element, Collector<String> out) throws Exception {

        // 1.拿到当前的key的数据
        Iterable<SensorInEntity> currentState = listTState.get();

        // 2.如果状态值还没有初始化，则初始化
        if (currentState == null) {
            listTState.addAll(Collections.emptyList());
        }

        // 3.更新状态
        listTState.add(element);

        // 4.判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        List<SensorInEntity> allElements = Lists.newArrayList(listTState.get());

        if (allElements.size() == 3) {
            long count = 0;
            long sum = 0;
            for (SensorInEntity ele : allElements) {
                count++;
                sum += Long.valueOf(ele.getValue());
            }
            double avg = (double) sum / count;
            DecimalFormat df = new DecimalFormat("#.00");
            avgValue = df.format(avg);

            String warningTitle = "输出："+"\n"+"异常检测结果："+"\n";
            if ((Double.valueOf(element.getValue()) - avg) > 3 ) {
                String sensorType = element.getSensorType();
                String sensorTime = element.getSensorTime();
                String valueT = element.getValue();
                String printInfo = warningTitle + sensorType+","+sensorTime+","+valueT+";"+"温度"+"过高";
                out.collect(printInfo);
                // 清除状态
                listTState.clear();
            }
            // 清除状态
            //listTState.clear();
        }

    }

    /**
     * 处理Q传感器数据
     * @param value
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap2(SensorInEntity value, Collector<String> out) throws Exception {

        // 拿到当前的 key 的状态值
        Iterable<SensorInEntity> currentState = listQState.get();

        // 如果状态值还没有初始化，则初始化
        if (currentState == null) {
            listQState.addAll(Collections.emptyList());
        }

        // 更新状态
        listQState.add(value);

        // 取到Q1数据
        List<SensorInEntity> allQElements = Lists.newArrayList(listQState.get());

        if (allQElements.size() == 5) {
            StringBuilder printInfo = new StringBuilder();
            String sensorType = null;
            String sensorTime;
            List<String> sensorTimeList = new ArrayList<>();
            List<Double> metricListAB = new ArrayList<>();
            List<Double> metricListAE = new ArrayList<>();
            List<Double> metricListCE = new ArrayList<>();
            // Q1,2020-01-30 19:30:10,AB:37.8,AE:100,CE:0.11
            for (int i = 0; i < allQElements.size(); i++) {
                // Q1
                sensorType = allQElements.get(i).getSensorType();
                // 2020-01-30 19:30:10
                sensorTime = allQElements.get(i).getSensorTime();
                // AB:37.8,AE:100,CE:0.11
                String[] values = allQElements.get(i).getValue().split(",");
                sensorTimeList.add(sensorTime);
                // [AB:37.8,AE:100,CE:0.11]
                for (int j = 0; j < values.length; j++) {
                    String[] metricStr = values[j].split(":");
                    String key = metricStr[0];
                    String val = metricStr[1];
                    if ("AB".equals(key)) {
                        // 取出AB指标值
                        Double metricValue = Double.valueOf(val);
                        metricListAB.add(metricValue);
                    }
                    if ("AE".equals(key)) {
                        // 取出AE指标值
                        Double metricValue = Double.valueOf(val);
                        metricListAE.add(metricValue);
                    }
                    if ("CE".equals(key)) {
                        // 取出CE指标值
                        Double metricValue = Double.valueOf(val);
                        metricListCE.add(metricValue);
                    }
                }
            }

            handleMetricValue(printInfo, sensorType, sensorTimeList, metricListAB, metricListAE, metricListCE);

            String printTitle = "报表结果：" + "\n";
            printInfo.append(printTitle + "温度：" + value.getSensorTime().substring(0, 10) + " " + avgValue);
            out.collect(printInfo.toString());
            // 清除状态
            listQState.clear();
        }
    }

    /**
     * 处理指标值
     * @param printInfo 打印的信息
     * @param sensorType 传感器类型
     * @param sensorTimeList 传感器采集时间列表
     * @param metricListAB AB指标列表
     * @param metricListAE AE指标列表
     * @param metricListCE CE指标列表
     */
    private void handleMetricValue(StringBuilder printInfo, String sensorType, List<String> sensorTimeList, List<Double> metricListAB, List<Double> metricListAE, List<Double> metricListCE) {
        for (int i = 0; i < metricListAB.size(); i++) {
            double dv = metricListAB.get(i) * 1.01;
            if (i+2 == metricListAB.size()) {
                break;
            }
            if (dv < metricListAB.get(i+1) && dv < metricListAB.get(i+2)) {
                printInfo.append(sensorType + "," + sensorTimeList.get(i+1) + "," + "酸度"+":"+ metricListAB.get(i+1) + " " + "第一次" + "酸度" + "过高" + "\n");
                printInfo.append(sensorType + "," + sensorTimeList.get(i+2) + "," + "酸度"+":"+ metricListAB.get(i+2) + " " + "第二次" + "酸度" + "过高" + "\n");
            }
        }
        for (int i = 0; i < metricListAE.size(); i++) {
            double dv = metricListAE.get(i) * 1.01;
            if (i+2 == metricListAE.size()) {
                break;
            }
            if (dv < metricListAE.get(i+1) && dv < metricListAE.get(i+2)) {
                printInfo.append(sensorType + "," + sensorTimeList.get(i+1) + "," + "粘稠度"+":"+ metricListAE.get(i+1) + " " + "第一次" + "粘稠度" + "过高" + "\n");
                printInfo.append(sensorType + "," + sensorTimeList.get(i+2) + "," + "粘稠度"+":"+ metricListAE.get(i+2) + " " + "第二次" + "粘稠度" + "过高" + "\n");
            }
        }
        for (int i = 0; i < metricListCE.size(); i++) {
            double dv = metricListCE.get(i) * 1.01;
            if (i+2 == metricListCE.size()) {
                break;
            }
            if (dv < metricListCE.get(i+1) && dv < metricListCE.get(i+2)) {
                printInfo.append(sensorType + "," + sensorTimeList.get(i+1) + "," + "含水量"+":"+ metricListCE.get(i+1) + " " + "第一次" + "含水量" + "过高" + "\n");
                printInfo.append(sensorType + "," + sensorTimeList.get(i+2) + "," + "含水量"+":"+ metricListCE.get(i+2) + " " + "第二次" + "含水量" + "过高" + "\n");
            }
        }
    }
}
