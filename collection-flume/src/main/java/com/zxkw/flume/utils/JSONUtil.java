package com.zxkw.flume.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lenovo
 * json工具类:判断字符串是否是合法的json：判断string字符串是否可以转为json
 */

public class JSONUtil {

    // json工具类
    public static boolean isJSONValidate(String log) {
        try {
            JSONObject.parseObject(log);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // 主方法测试,写入test文件夹中(绿色标识),实在不行就写入写入test包中,勿上传git
//    public static void main(String[] args) {
//        List<String> events = new ArrayList<>();
//        events.add("a"); //index = 0
//        events.add("b"); //index = 1
//        events.add("c"); //index = 2
//        events.add("d"); //index = 3
//
//        System.out.println(events);
//        events.remove(1);
//        System.out.println(events);
//        events.remove(3);
//        System.out.println(events);
//    }




}
