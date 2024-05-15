package com.zxkw.userprofile.app

import java.util.Properties
import com.zxkw.userprofile.common.bean.TagInfo
import com.zxkw.userprofile.common.dao.TagInfoDAO
import com.zxkw.userprofile.common.util.MyPropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TaskMerge {

//  1  创建标签宽表
//    一次创建还是 周期性创建
//    标签宽表的列取决于有多少个标签 ，所以每天的列 是动态变化
//    每天都要新建一个标签表    标签表（周期性表）： 表名前缀_日期
//  2  通过多个标签表
//    拼接为标签高表  ， 再利用pivot 进行行转列 转为标签宽表
//  3  把数据写入到宽表中

  def main(args: Array[String]): Unit = {
    //  1  创建标签宽表
    //    一次创建还是 周期性创建
    //    标签宽表的列取决于有多少个标签 ，所以每天的列 是动态变化
    //    每天都要新建一个标签表    标签表（周期性表）： 表名前缀_日期
    // create table tableName
    //  ( uid string , tag_code1  string , tag_code2 string ....)
    //  partitioned   因为每天建表 所以 不同分区
    //    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
    //   LOCATION  '$hdfsStorePath/$userProfileDbName/$tableName'
    val sparkConf: SparkConf = new SparkConf().setAppName("task_merge_app")//.setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


    val taskId: String = args(0)
    val taskDate: String = args(1)

    val tableName=s"user_tag_merge_${taskDate.replace("-","")}"
    // 标签宽表的列 取决于 有多少个 启用状态的标签 --> 通过查询mysql中的tag_info+task_info
    val tagInfoList: List[TagInfo] = TagInfoDAO.getTagInfoListTaskOn()
    val tagCodeList: List[String] = tagInfoList.map(tagInfo=> s" ${tagInfo.tagCode.toLowerCase} string")
    val tagCodeSQL: String = tagCodeList.mkString(",")


    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val userProfileDbName = properties.getProperty("user-profile.dbname")
    val wareHouseDbName = properties.getProperty("data-warehouse.dbname")
    val hdfsStorePath = properties.getProperty("hdfs-store.path")

   // 考虑幂等性  数据重试计算
    sparkSession.sql(s"drop table if exists  $userProfileDbName.$tableName")

    val createTableSQL=
      s"""
         |
         |      create  table if not exists $userProfileDbName.$tableName
         |      ( uid string , $tagCodeSQL )
         |      ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
         |     LOCATION  '$hdfsStorePath/$userProfileDbName/$tableName'
       """.stripMargin
    println(createTableSQL)
    sparkSession.sql(createTableSQL)


    //  2  通过多个标签表
    //    拼接为标签高表 union all  ， 再利用pivot 进行行转列 转为标签宽表
    // select * from
    // ( select uid ,'tag_code1' tag_code ,tag_value  from  tag_code1 where dt='taskDate'
    //  union all
    //  select uid ,'tag_code2' tag_code ,tag_value  from  tag_code2 where dt='taskDate'
    //  union all
    //  ...) tagtable
    //  pivot (     max( tag_value  ) as  tv   for     tag_code in ('tagcode1' ,'tagcode2','tagcode3'.. )     )


    //select uid ,'tag_code1' tag_code ,tag_value  from  tag_code1 where dt='taskDate'
    //     union all
    //      select uid ,'tag_code2' tag_code ,tag_value  from  tag_code2 where dt='taskDate'
    //     union all
    //     ...
    val tagSQLlist:List[String]=  tagInfoList.map { tagInfo =>
      s"""
         |select uid ,'${tagInfo.tagCode}' tag_code ,tag_value
         | from $userProfileDbName.${tagInfo.tagCode.toLowerCase}
         | where dt='$taskDate'
       """.
        stripMargin
      }
    val tagTableSQL: String = tagSQLlist.mkString(" union all ")

    //'tagcode1' ,'tagcode2','tagcode3'..
    val tagCodePivotSQL: String = tagInfoList.map(tagInfo=>s"'${tagInfo.tagCode}'").mkString(",")


     val  selectSQL=
       s"""
          |select * from
          |($tagTableSQL)
          |    tagtable
          |  pivot (     max( tag_value  ) as  tv   for  tag_code in ( $tagCodePivotSQL ))
        """.stripMargin
     println(selectSQL)


    //  3  把数据写入到宽表中

    val insertSQL=s" insert overwrite table $userProfileDbName.$tableName $selectSQL"
    println(insertSQL)
    sparkSession.sql(insertSQL)


  }





}
