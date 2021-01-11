package com.thoughtworks.business.service;

import com.thoughtworks.util.FileUtils;

import java.util.List;

/**
 * @author springdu
 * @create 2021/1/11 21:18
 * @description TODO
 */
public abstract class BaseIOTDao {

    /**
     * 读取输入源数据
     * @param fileName
     */
    protected void readIOTInputData(String fileName) {
        // 读取输入数据
        List<String> inputData = FileUtils.readFile(fileName);
        // 显示输入内容数据
        inputDisplay(inputData);
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
