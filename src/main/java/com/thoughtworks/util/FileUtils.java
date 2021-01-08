package com.thoughtworks.util;

import com.thoughtworks.constant.Constant;
import com.thoughtworks.exception.FileException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: spring du
 * @description: 文件工具类
 * @date: 2021/1/8 17:33
 */
public class FileUtils {

    /**
     * 读取文件
     * @param fileName
     * @return
     */
    public static List<String> readFile(String fileName) {
        List<String> list = new ArrayList<>();
        InputStream inputStream = FileUtils.class.getClassLoader().getResourceAsStream(fileName);
        if (null == inputStream) {
            String msg = "file not found: " + fileName;
            throw new FileException(msg);
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String lineStr;
        try {
            while (null != (lineStr = br.readLine())) {
                list.add(lineStr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }

    /**
     * 获取resources指定文件
     * @return
     */
    public static String getResourcePath() {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(Constant.INPUT_FILE_NAME);
        String path = resource.getPath();
        return path;
    }
}
