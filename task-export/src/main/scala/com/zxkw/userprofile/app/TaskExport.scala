package com.atguigu.userprofile.app

import java.util.Properties

import com.atguigu.userprofile.common.bean.TagInfo
import com.atguigu.userprofile.common.dao.TagInfoDAO
import com.atguigu.userprofile.common.util.{ClickhouseUtil, MyPropertiesUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object TaskExport {
//  1  clickhouse 中建立宽表
//      1  一次性建 周期建？  周期性 2  程序中建
//  2  从hive中读取数据
//
//  3   写入到clickhouse中

  def main(args: Array[String]): Unit = {

    //  1  clickhouse 中建立宽表
    //      1  一次性建 周期建？  周期性 2  程序中建
     //  create table $tableName
     //  (uid String ,tagcode1 String ,tagcode2 String ...)
    //   engine = MergeTree
    //   order by uid

    val sparkConf: SparkConf = new SparkConf().setAppName("task_export_app")//.setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


    val taskId: String = args(0)
    val taskDate: String = args(1)

    val tableName=s"user_tag_merge_${taskDate.replace("-","")}"
    // 标签宽表的列 取决于 有多少个 启用状态的标签 --> 通过查询mysql中的tag_info+task_info
    val tagInfoList: List[TagInfo] = TagInfoDAO.getTagInfoListTaskOn()
    val tagCodeList: List[String] = tagInfoList.map(tagInfo=> s" ${tagInfo.tagCode.toLowerCase} String")
    val tagCodeSQL: String = tagCodeList.mkString(",")


    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val userProfileDbName = properties.getProperty("user-profile.dbname")
    val wareHouseDbName = properties.getProperty("data-warehouse.dbname")
    val clickhouseUrl = properties.getProperty("clickhouse.url")

    var createTableSQL=
      s"""
         |     create table $tableName
         |       (  uid String ,$tagCodeSQL)
         |       engine = MergeTree
         |       order by uid
       """.stripMargin
    println(createTableSQL)
     ClickhouseUtil.executeSql(s"drop table if exists  $tableName")
     ClickhouseUtil.executeSql(createTableSQL)


    //  2  从hive中读取数据
   //rdd dataframe dataset
    println(s"select * from $tableName" )
    val dataFrame: DataFrame = sparkSession.sql(s"select * from $userProfileDbName.$tableName" )

    //driver  //executor

    //  3   写入到clickhouse中
   //   val rows: Array[Row] = dataFrame.collect()   // 从ex 提取到driver 中   无法分布式处理
   //  ClickhouseUtil.executeSql("insertxx" ,rows)
    println(s"写入clickhouse" )
    dataFrame.write.mode(SaveMode.Append)
        .option("batchsize",1000) //批量提交
        .option("isolationLevel", "NONE") //关闭事务
        .option("numPartitions",6)//设定并行度
       .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
      .jdbc(clickhouseUrl,tableName,new Properties())


  }
}
