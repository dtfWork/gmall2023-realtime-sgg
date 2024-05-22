package com.zxkw.userprofile.common.util;

import java.util.ResourceBundle;

public class MyPropsUtil {
    private static ResourceBundle bundle = ResourceBundle.getBundle("config");

    public static String get(String key){
        return  bundle.getString(key);
    }

    public static void main(String[] args) {
        System.out.println(get("mysql.url"));
    }
}
