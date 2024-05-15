package com.zxkw.userprofile.ml.train

import java.util.Properties
import com.zxkw.userprofile.common.util.MyPropertiesUtil
import com.zxkw.userprofile.ml.pipeline.MyPipeline
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


// 1   提取特征 sql（必须有label）    ->df
// 2    拆分数据集   ->train test
//3     初始化流水线工具
//4    利用流水线  进行训练
//5     测试
//6     评估
//7     保存
object GmallGenderTrain {


  def main(args: Array[String]): Unit = {

    // 0 环境
    println("0 环境")
    val taskId: String = args(0)
    val taskDate: String = args(1)

    val sparkConf: SparkConf = new SparkConf().setAppName("task_gmall_gender_train").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 1   提取特征 sql（必须有label）    ->df
    println("1   提取特征 sql（必须有label）    ->df")
    val sql=
      s"""
         |  with  uc
         |   as (
         |     select  user_id ,gender,category1_id  ,during_time
         |     from  (  select   pl.user_id,page_item ,during_time from  dwd_page_log  pl  where page_id='good_detail' and dt='$taskDate' )  uk
         |     inner join (select  id, category1_id from   dim_sku_info     where dt='$taskDate')  kc on uk.page_item= kc.id
         |     inner join ( select id,gender  from  dim_user_info  where dt='9999-99-99' and gender is not null)  ui  on    ui.id=uk.user_id
         |  )
         |  select  user_id ,gender ,
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
    val upName = properties.getProperty("user_profile0428")
    sparkSession.sql(s"use $dwName")
     val dataFrame: DataFrame = sparkSession.sql(sql)

    // 2    拆分数据集   ->train test
    println("2    拆分数据集")
     var Array(trainDF,testDF)= dataFrame.randomSplit(Array(0.8,0.2))

    //3     初始化流水线工具
    println("3     初始化流水线工具")
    val myPipeline: MyPipeline = new MyPipeline().setLabelColName("gender").setFeatureCols(Array("top1_c1", "top2_c1", "top3_c1", "male_dur", "female_dur"))
      .setMaxDepth(5).setMinInstancesPerNode(2).setMinInfoGain(0.01).setMaxBins(24).init()


    //4    利用流水线  进行训练
    println("4    利用流水线  进行训练")
    myPipeline.train(trainDF)
    myPipeline.printTree()
    myPipeline.printFeatureWeight()


    //5     测试
    println("5     测试")
    val predictedDF: DataFrame = myPipeline.predict(testDF)
    //6     评估
    println("6     评估")
    myPipeline.printEvaluate(predictedDF)
    //7     保存
    println("7     保存")
    // properties.getProperty("user_profile0428")
    val modelPath = properties.getProperty("model.path.gmall")
    myPipeline.saveModel(modelPath)


  }




}
