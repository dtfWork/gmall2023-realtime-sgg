package com.zxkw.userprofile.common.util;

import com.google.common.base.CaseFormat;
import com.zxkw.userprofile.common.constants.ConstCode;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MySqlUtil {
    static String url = MyPropsUtil.get(ConstCode.MYSQL_URL);
    static String username = MyPropsUtil.get(ConstCode.MYSQL_USERNAME);
    static String password = MyPropsUtil.get(ConstCode.MYSQL_PASSWORD);


    /**
     * 根据给定的SQL, 查询对应的元素
     */
    public static <T> T queryOne(String sql , Class<T> clazz, Boolean underScoreToCamel){
        List<T> queryList = queryList(sql, clazz, underScoreToCamel);
        if(queryList.size() > 0 ){
            return queryList.get(0);
        }else{
            return null ;
        }
    }

    /**
     *  根据给定的sql语句和class类型，查询对应的元素列表
     */
    public static <T> List<T> queryList(String sql , Class<T> clazz , Boolean underScoreToCamel){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection(url, username, password);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            ArrayList<T> resultList = new ArrayList<>();
            while(resultSet.next()){
                T obj = clazz.newInstance();
                for(int i = 1 ; i <=resultSetMetaData.getColumnCount() ; i ++ ){
                    String propertyName ;
                    if(underScoreToCamel){
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,resultSetMetaData.getColumnLabel(i));
                    }else{
                        propertyName = resultSetMetaData.getColumnLabel(i);
                    }
                    if(resultSet.getObject(i) != null ){
                        BeanUtils.setProperty(obj , propertyName , resultSet.getObject(i));
                    }
                }
                resultList.add(obj);
            }
            resultSet.close();
            preparedStatement.close();
            connection.close();
            return  resultList ;

        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询mysql失败：" + e.getMessage());
        }
    }
}
