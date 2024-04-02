package com.zxkw.flume.interceptor.userprofile.app

import java.util.Properties
import com.zxkw.flume.interceptor.userprofile.common.bean.{TagInfo, TaskInfo, TaskTagRule}
import com.zxkw.flume.interceptor.userprofile.common.constants.ConstCode
import com.zxkw.flume.interceptor.userprofile.common.dao.{TagInfoDAO, TaskInfoDAO, TaskTagRuleDAO}
import com.zxkw.flume.interceptor.userprofile.common.util.{MyPropertiesUtil, MySqlUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


////  负责执行所有SQL类型的标签任务
object TaskSQL {


  //  1   获得标签的定义、规则、SQL 、四级标签的映射关系
  //  从mysql中取
  //
  //  2    从数仓中读取数据
  //
  //  3   写入到某个标签表
  //    3.1 定义标签表
  //    3.2 写入标签表
  def main(args: Array[String]): Unit = {
    //  1   获得标签的定义、规则、SQL 、四级标签的映射关系
    //  从my想·sql中取
    //  1.1  如何查询 mysql  工具类
    //  1.2 查询对应的表  tag_info 、task_info、task_tag_rule
    val sparkConf: SparkConf = new SparkConf().setAppName("task_sql_app")//.setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val taskId: String = args(0)
    val taskDate: String = args(1)

    // tag_info
    val tagInfo: TagInfo = TagInfoDAO.getTagInfoByTaskId(taskId)
    println(tagInfo)
    //task_info
    val taskInfo: TaskInfo = TaskInfoDAO.getTaskInfo(taskId)
    println(taskInfo)
    //task_tag_rule
    val taskTagRuleList: List[TaskTagRule] = TaskTagRuleDAO.getTaskTagRuleListByTaskId(taskId)
    println(taskTagRuleList)

    //  2定义目标表  create table   ( 1  一次性建立  （只要表结构不变）   2 周期性执行     a手工 b 程序)
    //   hive
    val tableName = tagInfo.tagCode.toLowerCase()
    val tagValueType: String = tagInfo.tagValueType match {
      case ConstCode.TAG_VALUE_TYPE_STRING => "STRING"
      case ConstCode.TAG_VALUE_TYPE_LONG => "BIGINT"
      case ConstCode.TAG_VALUE_TYPE_DECIMAL => "DECIMAL(16,2)"
      case ConstCode.TAG_VALUE_TYPE_DATE => "STRING"
    }

    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val userProfileDbName = properties.getProperty("user-profile.dbname")
    val wareHouseDbName = properties.getProperty("data-warehouse.dbname")
    val hdfsStorePath = properties.getProperty("hdfs-store.path")

    //  create table  $tableName
    // 字段： ( uid string, tag_value $tagValueType     )
    //  partitioned by (dt string)
    //  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
    // LOCATION  $hdfsStorePath/$userProfileDbName/$tableName

    val createTableSQL =
      s"""
         |   create table if  not exists  $userProfileDbName.$tableName
         |    ( uid string, tag_value $tagValueType     )
         |    partitioned by (dt string)
         |   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
         |   LOCATION  '$hdfsStorePath/$userProfileDbName/$tableName'
     """.stripMargin

    println(createTableSQL)
    sparkSession.sql(createTableSQL)

    //3  组合select SQL 语句
    //  select uid ,  case  query_value  when 'F' then '女' ...  end
    //from  ($TAG_SQL)

    //根据四级标签的映射关系动态组合case when 子句
    // #作业：兼容 带四级标签和 不带四级标签的情况
    var tagValueSQL = ""
   if (taskTagRuleList.size>0){
    val whenThenList: List[String] = taskTagRuleList.map(taskTagRule => s" when  '${taskTagRule.queryValue}' then  '${taskTagRule.subTagValue}' ")
    val whenThenSQL: String = whenThenList.mkString(" ")
     tagValueSQL = s"case query_value $whenThenSQL end "

   }else{
     tagValueSQL =" query_value "
   }
    //#作业 替换sql中的日期标识
    val tagsql: String = taskInfo.taskSql.replace("$dt",taskDate)
    val selectSQL = s"  select uid ,  $tagValueSQL as tag_value from (${tagsql}) tagsql"
    println(selectSQL)

    //4 组合插入sql
    sparkSession.sql(s"use $wareHouseDbName")
    val insertSQL = s"  insert overwrite table $userProfileDbName.$tableName partition (dt='$taskDate') $selectSQL"
    println(insertSQL)
    sparkSession.sql(insertSQL)
  }

}
