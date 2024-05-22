package com.zxkw.userprofile.app;

import com.zxkw.userprofile.common.bean.TagInfo;
import com.zxkw.userprofile.common.constants.ConstCode;
import com.zxkw.userprofile.common.dao.TagInfoDao;
import com.zxkw.userprofile.common.util.MyPropsUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 合并标签
 * 1. 取得计算的业务日期
 * 2. 查询Mysql得出所有的需要被合并的标签，其中使用tag_code作为列名
 * 3. 建立宽表, 不能手工建立， 因为不确定有多少列， 每天可能不一样
 * 4. 通过一条pivot SQL语句把多个标签单表合并成为一个标签宽表
 *        维度列： uid  旋转列: tag_code    聚合列: tag_value
 */
public class TaskMerge {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("task_merge_app") .setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();


        //1.取得计算的业务日期
        String taskId = args[0];
        String businessDate = args[1];

        //2. 查询Mysql得出所有的需要被合并的标签，其中使用tag_code作为列名
        List<TagInfo> tagInfoList = TagInfoDao.getTagInfoList();

        //3. 建立宽表, 不能手工建立， 因为不确定有多少列， 每天可能不一样
        //  表名?  字段?  分区?  存储位置?  格式?
        // create table if not exists up_tag_merge_[businessDate]
        // (
        // uid string ,
        // [tagCodeSQL(tagCode string )]
        // )
        // row format delimited fields terminated by  '\\t'
        // location '[HDFS_PATH]/[UP_DBNAME]/[TABLENAME]'

        String tableName = "up_tag_merge_" + businessDate.replace("-" , "");
        String hdfsPath = MyPropsUtil.get(ConstCode.HDFS_STORE_PATH);
        String upDbName = MyPropsUtil.get(ConstCode.USER_PROFILE_DBNAME);

        List<String> tagCodeSQLList =
                tagInfoList.stream().map(tagInfo -> tagInfo.getTagCode().toLowerCase() + " string ").collect(Collectors.toList());
        String tagCodeSQL = StringUtils.join(tagCodeSQLList, ",");
        String dropTableSQL = " drop table if exists " + upDbName + "." + tableName;
        System.out.println("dropTableSQL : " + dropTableSQL );
        sparkSession.sql(dropTableSQL);
        String  createTableSQL =  " create table if not exists " + upDbName+ "." + tableName +
                " (" +
                " uid string, "  + tagCodeSQL +
                " )" +
                " row format delimited fields terminated by '\\t'" +
                " location '"+hdfsPath+ "/" + upDbName +"/" + tableName + "'";

        System.out.println("createTableSQL : " + createTableSQL);
        sparkSession.sql(createTableSQL);

        //4. 通过一条pivot SQL语句把多个标签单表合并成为一个标签宽表
        // 维度列： uid  旋转列: tag_code    聚合列: tag_value
        List<String> tagSqlList = tagInfoList.stream().map(
                tagInfo -> "select uid , '"
                        + tagInfo.getTagCode().toLowerCase() +
                        "' as tag_code , tag_value from " +
                        upDbName + "." + tagInfo.getTagCode().toLowerCase() +
                        " where dt = '" + businessDate + "'").collect(Collectors.toList());
        String unionSQL = StringUtils.join(tagSqlList, " union all ");

        List<String> tagCodeList =
                tagInfoList.stream().map(tagInfo -> "'" + tagInfo.getTagCode().toLowerCase() + "'").collect(Collectors.toList());
        String tagCodeQuerySQL = StringUtils.join(tagCodeList, ",");

        String pivotSQL = " select * from ( " + unionSQL + ") "+
                " pivot (max(tag_value) tv for tag_code in ("+ tagCodeQuerySQL+"))" ;

        System.out.println("pivotSQL: " + pivotSQL);

        String insertSQL = " insert overwrite table "+ upDbName+ "."+ tableName + " " + pivotSQL ;
        System.out.println("insertSQL :" + insertSQL);

        sparkSession.sql(insertSQL);

    }
}
