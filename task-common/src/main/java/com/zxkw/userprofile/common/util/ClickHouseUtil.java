package com.zxkw.userprofile.common.util;

import com.zxkw.userprofile.common.constants.ConstCode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 执行clickhouse的Sql语句的工具类
 * 该工具也会被其他模块使用，所以放在task-common模块下
 */
public class ClickHouseUtil {
    private  static String url = MyPropsUtil.get(ConstCode.CLICKHOUSE_URL);

    public static void executeSql(String sql ) {
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            Connection connection = DriverManager.getConnection(url ,null ,null );
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        }catch(ClassNotFoundException e){
            e.printStackTrace();
            throw new RuntimeException("找不到驱动");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("获取clickhouse连接失败");
        }
    }
}
