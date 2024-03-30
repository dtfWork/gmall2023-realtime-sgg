package com.atguigu.userprofile.ml.train

import java.util.Properties

import com.atguigu.userprofile.common.util.MyPropertiesUtil
import com.atguigu.userprofile.ml.pipeline.MyPipeline
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object StudentGenderTrain {


//  0  spark hive环境
//  1  提取数据  sql   dataframe
//  2  数据分成 训练集 和 测试集
//  3   创建MyPipeline ，并初始化设参数。
//  4   用MyPipeline对训练集进行训练
//    5    再用MyPipeline（含模型）进行预测

  def main(args: Array[String]): Unit = {

    //  0  spark hive环境
    println(" 0  spark hive环境")
    val sparkConf: SparkConf = new SparkConf().setAppName("task_student_gender_train").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //  1  提取数据  sql   dataframe
    println(" 1 提取数据  sql  -> dataframe")
   val sql=
     s"""
        |select uid ,
        | case  hair  when  '长发' then 10
        |           when  '短发' then 11
        |           when  '板寸' then 12 end  hair  ,
        | height ,
        | case   skirt   when  '是' then 21
        |                 when '否'  then 20  end skirt ,
        | case  age   when  '80后' then 80
        |             when  '90后' then 90
        |             when  '00后' then 100 end age ,
        | gender
        | from user_profile2077.student
      """.stripMargin
     val dataFrame: DataFrame = sparkSession.sql(sql)
    //  2  数据分成 训练集 和 测试集
    println("    2  数据分成 训练集 和 测试集")
    val Array(trainDF,testDF) = dataFrame.randomSplit(Array(0.7,0.3))

    //  3   创建MyPipeline ，并初始化设参数。
    println("   3   创建MyPipeline ，并初始化设参数。")
     val myPipeline: MyPipeline = new MyPipeline()
       .setLabelColName("gender").setFeatureCols(Array("hair","height","skirt","age"))
       .setMaxCategories(5)
       .setMaxDepth(5)
       .setMinInstancesPerNode(3)
       .setMinInfoGain(0.02)
       .setMaxBins(20)
       .init()

    //  4   用MyPipeline对训练集进行训练
    println(" 4.1  用MyPipeline对训练集进行训练")
    myPipeline.train(trainDF)

    println(" 4.2   打印决策树")
    myPipeline.printTree()

    println(" 4.3  打印特征权重")
    myPipeline.printFeatureWeight()

    //    5    再用MyPipeline（含模型）进行预测
    println(" 5    再用MyPipeline（含模型）进行预测")
    val predictedDF: DataFrame = myPipeline.predict(testDF)

    predictedDF.show(1000,false)


    // 打印评估报告
    println(" 6   打印评估报告")
    myPipeline.printEvaluate(predictedDF)

    // 把矢量值转为原值
    println(" 7   把矢量值转为原值")
    val convertedDF: DataFrame = myPipeline.convertOrigin(predictedDF)
    convertedDF.show(1000,false)

    //存储模型
    println(" 8   存储模型")
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val modelPath = properties.getProperty("model.path")
    myPipeline.saveModel(modelPath)


  }

}
