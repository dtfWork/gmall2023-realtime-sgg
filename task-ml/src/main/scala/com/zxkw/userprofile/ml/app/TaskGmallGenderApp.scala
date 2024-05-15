package com.zxkw.userprofile.ml.app

import java.util.Properties
import com.zxkw.userprofile.common.bean.TagInfo
import com.zxkw.userprofile.common.dao.TagInfoDAO
import com.zxkw.userprofile.common.util.MyPropertiesUtil
import com.zxkw.userprofile.ml.pipeline.MyPipeline
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object TaskGmallGenderApp {

//1  提取数据 (答案随意)
//2  加载模型到流水线
//3   用流水线进行预测
//4   把矢量值转为原值
  def main(args: Array[String]): Unit = {
      println(" 0  spark hive环境")

  val taskId: String = args(0)
  val taskDate: String = args(1)

      val sparkConf: SparkConf = new SparkConf().setAppName("task_gmall_gender_app")//.setMaster("local[*]")
      val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
     //1  提取数据 (全部用户)
  println(" 1  提取数据 (全部用户)")
     val sql=
       s"""
          |  with  uc
          |   as (
          |     select  user_id ,gender,category1_id  ,during_time
          |     from  (  select   pl.user_id,page_item ,during_time from  dwd_page_log  pl  where page_id='good_detail' and dt='$taskDate' )  uk
          |     inner join (select  id, category1_id from   dim_sku_info     where dt='$taskDate')  kc on uk.page_item= kc.id
          |     inner join ( select id,gender  from  dim_user_info  where dt='9999-99-99'  )  ui  on    ui.id=uk.user_id
          |  )
          |  select  user_id ,
          |  sum(if(rk=1 ,category1_id,0))  top1_c1,
          |  sum(if(rk=2 ,category1_id,0))  top2_c1,
          |  sum(if(rk=3 ,category1_id,0) )  top3_c1,
          |   sum( if(category1_id in ('3','6','4'),  during_time,0))    male_dur,
          |     sum( if(category1_id in ('8','12','15'),  during_time,0))  female_dur
          |  from (
          |   select  user_id ,gender ,category1_id ,count(*) ,sum(during_time) during_time ,
          |   row_number()over(partition by  user_id ,gender  order by count(*) desc , sum(during_time) desc )  rk
          |   from uc
          |   group by  user_id ,gender ,category1_id
          |   order by user_id,category1_id
          |  ) uc_count
          |  group by   user_id ,gender
          |
       |
       |
     """.stripMargin

    val properties: Properties = MyPropertiesUtil.load("config.properties")

    val dwName = properties.getProperty("data-warehouse.dbname")
    val upName = properties.getProperty("user-profile.dbname")
    sparkSession.sql(s"use $dwName")
    val dataFrame: DataFrame = sparkSession.sql(sql)


     //2  加载模型到流水线
  println("2  加载模型到流水线")
     val modelPath = properties.getProperty("model.path.gmall")
      val myPipeline: MyPipeline = new MyPipeline().loadModel(modelPath)
     //3   用流水线进行预测
  println("3   用流水线进行预测")
     val predictedDF: DataFrame = myPipeline.predict(dataFrame)
      predictedDF.show(1000,false)

      //4   把矢量值转为原值
     println("4   把矢量值转为原值")
     val convertedDF: DataFrame = myPipeline.convertOrigin(predictedDF)
      convertedDF.show(1000,false)

       insertTag(convertedDF ,sparkSession ,upName:String, taskId ,taskDate ,properties )

    }

  //把预测后的结果写入标签表
//1   先有标签表
//2   提取出 uid-预测结果
//3   写入标签表
     def insertTag(convertedDF:DataFrame,sparkSession: SparkSession,userProfileDbName:String,
                   taskId:String ,taskDate:String,properties:Properties): Unit ={
     //1   创建标签表
     //
      val tagInfo: TagInfo = TagInfoDAO.getTagInfoByTaskId(taskId)
      val tableName=tagInfo.tagCode.toLowerCase

    val hdfsStorePath: String = properties.getProperty("hdfs-store.path")

     val createTableSQL =
     s"""
        |   create table if  not exists  $userProfileDbName.$tableName
        |    ( uid string, tag_value String     )
        |    partitioned by (dt string)
        |   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
        |   LOCATION  '$hdfsStorePath/$userProfileDbName/$tableName'
     """.stripMargin

    println(createTableSQL)
    sparkSession.sql(createTableSQL)

     convertedDF.createTempView("v_prediction")


      val insertSQL=
        s"""
           |  insert overwrite table $userProfileDbName.$tableName partition (dt='$taskDate')
           |  select  user_id ,  case  prediction_origin when 'M'  then '男'
           |                                             when 'F'   then '女' end  gender
           |                                             from v_prediction
         """.stripMargin
      println(insertSQL)
     sparkSession.sql(insertSQL)



   }


}
