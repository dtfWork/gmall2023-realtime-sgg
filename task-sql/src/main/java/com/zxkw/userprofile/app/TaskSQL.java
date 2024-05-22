package com.zxkw.userprofile.app;

import com.zxkw.userprofile.common.bean.TagInfo;
import com.zxkw.userprofile.common.bean.TaskInfo;
import com.zxkw.userprofile.common.bean.TaskTagRule;
import com.zxkw.userprofile.common.constants.ConstCode;
import com.zxkw.userprofile.common.dao.TagInfoDao;
import com.zxkw.userprofile.common.dao.TaskInfoDao;
import com.zxkw.userprofile.common.dao.TaskTagRuleDao;
import com.zxkw.userprofile.common.util.MyPropsUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * 开发任务：根据SQL定义计算标签
 * 1. 接收spark-submit中传递的业务参数， 也就是task_id
 * 2. 根据task_id 查询mysql, 获得对应的task_info 、tag_info 、 task_tag_rule信息
 *    也就是标签名称、标签编码、定义的sql 、 获得计算值与标签值的映射关系
 * 3. 生成标签前, 先要检查是否有该标签的表， 如果没有， 要为给标签建立hive数据表
 * 4. 根据sql 和映射 , 动态拼接一个 insert select SQL
 * 5. 根据执行该sql
 */
public class TaskSQL {
    public static void main(String[] args) {
        //0. 执行环境
        SparkConf sparkConf = new SparkConf().setAppName("task_sql_app")
                //仅适用于idea本地;测试或生产集群运行需注释
                .setMaster("local[*]");
            //创建hive支持
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        //1. 接收spark-submit中传递的业务参数， 也就是task_id
        //spark-submit -xx -xx --class xxx  xxx.jar  101  2020-06-14
        //约定第一个参数放task_id , 第二参数方business_date
        String taskId =  args[0];
        String businessDate = args[1];

        //2. 根据task_id查询mysql , 获得对应的task_info , tag_info , task_tag_rule信息
        TagInfo tagInfo = TagInfoDao.getTagInfoByTaskId(taskId);
        System.out.println(tagInfo);
        TaskInfo taskInfo = TaskInfoDao.getTaskInfoById(taskId);
        System.out.println(taskInfo);
        List<TaskTagRule> taskTagRuleList = TaskTagRuleDao.getTaskTagRuleList(taskId);
        System.out.println(taskTagRuleList);

        String tagCode = tagInfo.getTagCode();
        String tagName = tagInfo.getTagName();
        String taskSql = taskInfo.getTaskSql();

        //TODO 补充: 替换SQL中$dt
        taskSql = taskSql.replace("$dt", businessDate);

        //3. 生成标签前,先要检查是否有该标签的表， 如果没有, 要为该标签建立数据表
        //   每张表负责一个标签, 两个字段:  谁 , 值是什么 , 比如性别标签表： uid ,tag_value  , 分区字段 busiDate
        // create table tableName[tagCode]
        // ( uid string , tag_value tagValueType?)
        // partitioned by (dt string)
        // ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
        // location 'hdfs标签主目录/画像库名/表名'

        // 表名 和 表描述
        String tableName = tagCode.toLowerCase();
        String comment = tagName ;

        String fieldType = null ;
        //确定字段类型
        switch (tagInfo.getTagValueType()){
            case ConstCode.TAG_VALUE_TYPE_LONG :
                fieldType = "bigint";
                break ;
            case ConstCode.TAG_VALUE_TYPE_DECIMAL:
                fieldType = "decimal(16,2)";
                break ;
            case ConstCode.TAG_VALUE_TYPE_STRING :
                fieldType = "string";
                break ;
            case ConstCode.TAG_VALUE_TYPE_DATE :
                fieldType = "string";
                break ;
        }

        String hdfsPath = MyPropsUtil.get(ConstCode.HDFS_STORE_PATH);
        String dwDbName = MyPropsUtil.get(ConstCode.DATA_WAREHOUSE_DBNAME);
        String upDbName = MyPropsUtil.get(ConstCode.USER_PROFILE_DBNAME);

        String createTableSql =
                " create table if not exists " + upDbName + "." + tableName +
                        " (" +
                        " uid string , " +
                        " tag_value " + fieldType +
                        " )" +
                        " partitioned by (dt string) " +
                        " row format delimited fields terminated by '\\t'" +
                        " location '" + hdfsPath+ "/" + upDbName + "/" + tableName + "'";

        System.out.println("createTableSQL:" + createTableSql);
        sparkSession.sql(createTableSql);

        //4. 要根据sql和映射， 动态拼接一个insert select sql
        // select uid , case query_value when 'F' then '女' when 'M' then '男' when 'U' then '未知'
        // from
        // (
        // select id as uid , if(gender <> '' , gender, 'U') as query_value from dim_user_zip where dt = '9999-12-31'
        // )

        // streamAPI
        //List<String> whenThenList = taskTagRuleList.stream().map(
        //        taskTagRule -> "when '" + taskTagRule.getQueryValue() + "' then '" + taskTagRule.getSubTagValue() + "'"
        //).collect(Collectors.toList());

        //普通代码

        //TODO  判断是否有四级标签
        String queryValueSql = "";
        if(taskTagRuleList.size() > 0 ){
            //TODO  如果有四级标签，完成标签值到映射转换
            //拼接case when then end
            String caseWhenThenString  = "case query_value " ;
            for (TaskTagRule taskTagRule : taskTagRuleList) {
                caseWhenThenString += (" when '" +taskTagRule.getQueryValue() + "' then '" + taskTagRule.getSubTagValue() + "'");
            }
            caseWhenThenString += " end as query_value ";
            queryValueSql = caseWhenThenString ;
        }else{
            //TODO  如果没有四级标签, 查询值即为标签值
            queryValueSql = " query_value " ;
        }

        // 拼接SelectSQL
        String selectSQL = " select uid , " + queryValueSql  + " from (" +  taskSql  + ")" ;
        System.out.println("selectSQL:" + selectSQL);

        // 拼接InsertSQL
        String insertSQL = " insert overwrite table " + upDbName + "." + tableName + " partition (dt = '"+ businessDate+"') " + selectSQL;
        System.out.println("insertSQL:" + insertSQL );
        sparkSession.sql("use "+ dwDbName );
        sparkSession.sql(insertSQL);
    }
}
