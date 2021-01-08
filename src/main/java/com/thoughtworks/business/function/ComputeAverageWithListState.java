package com.thoughtworks.business.function;

import com.thoughtworks.constant.Constant;
import com.thoughtworks.entity.SensorInEntity;
import com.thoughtworks.enums.MetricTypeEnum;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author: spring du
 * @description: 列表状态处理逻辑类
 * @date: 2021/1/8 18:06
 */
@NoArgsConstructor
public class ComputeAverageWithListState extends RichFlatMapFunction<SensorInEntity, String> {

    /**
     * 定义ListState状态变量
     */
    private ListState<SensorInEntity> elementsByKey;

    /**
     * 平均值
     */
    private Double avg;

    /**
     * T类型传感器数据条数
     */
    private Integer tNum;

    /**
     * Q类型传感器数据条数
     */
    private Integer qNum;

    public ComputeAverageWithListState(Integer tNum, Integer qNum) {
        this.tNum = tNum;
        this.qNum = qNum;
    }

    /**
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ListStateDescriptor<SensorInEntity> descriptor =
                new ListStateDescriptor<>(
                        Constant.STATE_NAME, // 状态的名字
                        TypeInformation.of(SensorInEntity.class)); // 状态存储的数据类型
        elementsByKey = getRuntimeContext().getListState(descriptor);

    }

    /**
     * 这个方法是一个初始化的方法，只会执行一次
     * 用来注册状态
     * @param element
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(SensorInEntity element, Collector<String> out) throws Exception {

        if (MetricTypeEnum.T.getCode().equals(element.getSensorType())) {

            // 拿到当前的 key 的状态值
            Iterable<SensorInEntity> currentState = elementsByKey.get();

            // 如果状态值还没有初始化，则初始化
            if (currentState == null) {
                elementsByKey.addAll(Collections.emptyList());
            }

            // 更新状态
            elementsByKey.add(element);

            // 判断，如果当前的 key 出现了 3 次，计算平均值，并且输出
            ArrayList<SensorInEntity> allTElements = Lists.newArrayList(elementsByKey.get());

            if (allTElements.size() == tNum) {
                long count = 0;
                long sum = 0;
                for (SensorInEntity ele : allTElements) {
                    count++;
                    sum += Long.valueOf(ele.getValue());
                }

                String warningTitle = "输出："+Constant.LINE+"异常检测结果："+Constant.LINE;
                avg = (double) sum / count;
                if ((Double.valueOf(element.getValue()) - avg) > Constant.AVG_THRESHOLD ) {

                    String printInfo = warningTitle + element.getSensorType()+Constant.COMMA+element.getSensorTime()+Constant.COMMA+element.getValue()+Constant.SEMICOLON+"温度"+MetricTypeEnum.HIGH.getDesc();
                    out.collect(printInfo);
                    // 清除状态
                    elementsByKey.clear();
                }
            }
        } else if (MetricTypeEnum.Q.getCode().equals(element.getSensorType())) {

            StringBuilder printInfo = new StringBuilder();

            // 拿到当前的 key 的状态值
            Iterable<SensorInEntity> currentState = elementsByKey.get();

            // 如果状态值还没有初始化，则初始化
            if (currentState == null) {
                elementsByKey.addAll(Collections.emptyList());
            }

            // 更新状态
            elementsByKey.add(element);

            // 取到Q1数据
            ArrayList<SensorInEntity> allQElements = Lists.newArrayList(elementsByKey.get());

            if (allQElements.size() == qNum) {
                String sensorType = null;
                String sensorTime;
                List<String> sensorTimeList = new ArrayList<>();
                List<Double> metricListAB = new ArrayList<>();
                List<Double> metricListAE = new ArrayList<>();
                List<Double> metricListCE = new ArrayList<>();
                for (int i = 0; i < allQElements.size(); i++) {
                    sensorType = allQElements.get(i).getSensorType();
                    sensorTime = allQElements.get(i).getSensorTime();
                    String[] values = allQElements.get(i).getValue().split(Constant.COMMA);
                    sensorTimeList.add(sensorTime);
                    for (int j = 0; j < values.length; j++) {
                        String[] metricStr = values[j].split(Constant.COLON);
                        if (MetricTypeEnum.AB.getCode().equals(metricStr[0])) {
                            // 取出AB指标值
                            Double metricValue = Double.valueOf(metricStr[1]);
                            metricListAB.add(metricValue);
                        }
                        if (MetricTypeEnum.AE.getCode().equals(metricStr[0])) {
                            // 取出AE指标值
                            Double metricValue = Double.valueOf(metricStr[1]);
                            metricListAE.add(metricValue);
                        }
                        if (MetricTypeEnum.CE.getCode().equals(metricStr[0])) {
                            // 取出CE指标值
                            Double metricValue = Double.valueOf(metricStr[1]);
                            metricListCE.add(metricValue);
                        }
                    }
                }

                handleMetricValue(printInfo, sensorType, sensorTimeList, metricListAB, metricListAE, metricListCE);

                String printTitle = "报表结果：" + Constant.LINE;
                DecimalFormat df = new DecimalFormat("#.00");
                printInfo.append(printTitle + "温度：" + element.getSensorTime().substring(0,10) + Constant.SPACE + df.format(avg));
                out.collect(printInfo.toString());
                // 清除状态
                elementsByKey.clear();

            }

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
                printInfo.append(sensorType + Constant.COMMA + sensorTimeList.get(i+1) + Constant.COMMA + MetricTypeEnum.AB.getDesc()+Constant.COLON+ metricListAB.get(i+1) + Constant.SPACE + MetricTypeEnum.FIRST.getDesc() + MetricTypeEnum.AB.getDesc() + MetricTypeEnum.HIGH.getDesc() + Constant.LINE);
                printInfo.append(sensorType + Constant.COMMA + sensorTimeList.get(i+2) + Constant.COMMA + MetricTypeEnum.AB.getDesc()+Constant.COLON+ metricListAB.get(i+2) + Constant.SPACE + MetricTypeEnum.SECOND.getDesc() + MetricTypeEnum.AB.getDesc() + MetricTypeEnum.HIGH.getDesc() + Constant.LINE);
            }
        }
        for (int i = 0; i < metricListAE.size(); i++) {
            double dv = metricListAE.get(i) * 1.01;
            if (i+2 == metricListAE.size()) {
                break;
            }
            if (dv < metricListAE.get(i+1) && dv < metricListAE.get(i+2)) {
                printInfo.append(sensorType + Constant.COMMA + sensorTimeList.get(i+1) + Constant.COMMA + MetricTypeEnum.AE.getDesc()+Constant.COLON+ metricListAE.get(i+1) + Constant.SPACE + MetricTypeEnum.FIRST.getDesc() + MetricTypeEnum.AE.getDesc() + MetricTypeEnum.HIGH.getDesc() + Constant.LINE);
                printInfo.append(sensorType + Constant.COMMA + sensorTimeList.get(i+2) + Constant.COMMA + MetricTypeEnum.AE.getDesc()+Constant.COLON+ metricListAE.get(i+2) + Constant.SPACE + MetricTypeEnum.SECOND.getDesc() + MetricTypeEnum.AE.getDesc() + MetricTypeEnum.HIGH.getDesc() + Constant.LINE);
            }
        }
        for (int i = 0; i < metricListCE.size(); i++) {
            double dv = metricListCE.get(i) * 1.01;
            if (i+2 == metricListCE.size()) {
                break;
            }
            if (dv < metricListCE.get(i+1) && dv < metricListCE.get(i+2)) {
                printInfo.append(sensorType + Constant.COMMA + sensorTimeList.get(i+1) + Constant.COMMA + MetricTypeEnum.CE.getDesc()+Constant.COLON+ metricListCE.get(i+1) + Constant.SPACE + MetricTypeEnum.FIRST.getDesc() + MetricTypeEnum.CE.getDesc() + MetricTypeEnum.HIGH.getDesc() + Constant.LINE);
                printInfo.append(sensorType + Constant.COMMA + sensorTimeList.get(i+2) + Constant.COMMA + MetricTypeEnum.CE.getDesc()+Constant.COLON+ metricListCE.get(i+2) + Constant.SPACE + MetricTypeEnum.SECOND.getDesc() + MetricTypeEnum.CE.getDesc() + MetricTypeEnum.HIGH.getDesc() + Constant.LINE);
            }
        }
    }
}
