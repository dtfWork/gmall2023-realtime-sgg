package com.zxkw.flume.interceptor.userprofile.ml.pipeline

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class MyPipeline {


  var pipeline:Pipeline=null

  var pipelineModel:PipelineModel=null

  def  init(): MyPipeline ={
    pipeline= new Pipeline().setStages(Array(
        createLabelIndexer(),
        createFeatureAssembler(),
        createFeatureIndexer(),
        createClassifier()
    ))
    this
  }

  var  labelColName:String=null
  def  setLabelColName(labelColName:String): MyPipeline ={
    this.labelColName=labelColName
    this
  }

  var  featureCols:Array[String]=null

  def  setFeatureCols(featureCols :Array[String]): MyPipeline ={
    this.featureCols=featureCols
     this
  }

  var maxCategories=5
  def  setMaxCategories(maxCategories:Int): MyPipeline ={
      this.maxCategories=maxCategories
    this
  }


  //// 以下为参数 ////////////////////

  // 最大分支数
  private var maxBins=20
  // 最大树深度
  private var maxDepth=5
  //最小分支包含数据条数
  private var minInstancesPerNode=1
  //最小分支信息增益
  private var minInfoGain=0.0





  def setMaxBins(maxBins:Int): MyPipeline ={
    this.maxBins=maxBins
    this
  }
  def setMaxDepth(maxDepth:Int): MyPipeline ={
    this.maxDepth=maxDepth
    this
  }

  def setMinInstancesPerNode(minInstancesPerNode:Int): MyPipeline ={
    this.minInstancesPerNode=minInstancesPerNode
    this
  }

  def setMinInfoGain(minInfoGain:Double): MyPipeline ={
    this.minInfoGain=minInfoGain
    this
  }



  //1  大师兄
  //  标签索引 （参考答案)
  //  把标签的原始值 转换为矢量值
  //  矢量值 ：  0 ，1,2,3,4    //按出现的概率 分配  概率越大 矢量值越小
  //   setInputCol  给标签索引的列名
  //   setOutputCol  矢量化的标签索引列  ，自定义名称
  def createLabelIndexer(): StringIndexer ={
        val indexer = new StringIndexer()
       indexer.setInputCol(labelColName)
       indexer.setOutputCol("label_index")
       indexer
  }

  // 2  二师兄
  //  特征聚合
  //  把多个特征合并为一个特征
 //  setInputCols    特征列名数组
  // setOutputCol  聚合后的特征
  def  createFeatureAssembler(): VectorAssembler ={
          val assembler = new VectorAssembler()
        assembler.setInputCols(featureCols)
        assembler.setOutputCol("feature_assemble")
        assembler
  }


  // 3 三师弟
  //  特征索引
  //  把特征矢量化
  //  矢量值 ：  0 ，1,2,3,4    //按出现的概率 分配  概率越大 矢量值越小
  def createFeatureIndexer(): VectorIndexer ={
       val vectorIndexer = new VectorIndexer()
       vectorIndexer.setInputCol("feature_assemble")
       vectorIndexer.setOutputCol("feature_index")
    //区分 连续值特征和 离散特征  小于等于此值的样本个数 视为 离散特征 其他视为连续值特征
    //连续值特征不会被矢量化
       vectorIndexer.setMaxCategories(maxCategories)
       vectorIndexer.setHandleInvalid("keep")  //1 error 默认直接报错  //2 skip 跳过改行 //3 keep 如果遇到错误 保改行，放弃某个错误的特征
    vectorIndexer
  }

  // 师父
  // 分类器
  // setLabelCol   标签列  矢量化
  // setFeaturesCol  特征征列  矢量化
  // setPredictionCol  预测结果列

  def  createClassifier(): DecisionTreeClassifier ={
          val classifier = new DecisionTreeClassifier()
        classifier.setLabelCol("label_index")
        classifier.setFeaturesCol("feature_index")

        classifier.setPredictionCol("prediction")
        classifier.setImpurity("gini")

       classifier.setMaxDepth(maxDepth)
       classifier.setMinInstancesPerNode(minInstancesPerNode)
       classifier.setMinInfoGain(minInfoGain)
       classifier.setMaxBins(maxBins)

        classifier

  }


  // 训练
  def  train(dataFrame:DataFrame): Unit ={
    pipelineModel = pipeline.fit(dataFrame)
  }

   //预测
  def  predict(dataFrame:DataFrame): DataFrame ={
    val predictedDataFrame: DataFrame = pipelineModel.transform(dataFrame)
    predictedDataFrame
  }


  //  打印决策树
  def printTree(): Unit ={
    val transformer: Transformer = pipelineModel.stages(3)
    val classificationModel: DecisionTreeClassificationModel = transformer.asInstanceOf[DecisionTreeClassificationModel]
    println(classificationModel.toDebugString)
  }


  //   打印特征权重
  def printFeatureWeight(): Unit ={
    val transformer: Transformer = pipelineModel.stages(3)
    val classificationModel: DecisionTreeClassificationModel = transformer.asInstanceOf[DecisionTreeClassificationModel]
    println(classificationModel.featureImportances)
  }

  // 评估
  def printEvaluate(predictedDataFrame:DataFrame): Unit ={
    val rdd: RDD[(Double, Double)] = predictedDataFrame.rdd.map { row =>
      val label: Double = row.getAs[Double]("label_index")
      val prediction: Double = row.getAs[Double]("prediction")
      (label, prediction)
    }
     val metrics = new  MulticlassMetrics(rdd)
    val accuracy: Double = metrics.accuracy
    println(s"总准确率：$accuracy")

    metrics.labels.foreach { label=>
      println(s"标签：$label 的召回率 ${metrics.recall(label)}")
      println(s"标签：$label 的精确率 ${metrics.precision(label)}")
    }

  }



   // 模型存储
   def saveModel(path:String): Unit ={
     pipelineModel.write.overwrite().save(path)
   }



   //  模型加载
   def loadModel(path:String):MyPipeline= {
     pipelineModel = PipelineModel.load(path)
      this
   }


  // 矢量值转换为原值
  // setLabels  从大师兄处得到 矢量值与原值的映射关系
  //   setInputCol  预测后矢量值的列名
   // setOutputCol   转换后原值的列名
  def  convertOrigin(predictedDataFrame:DataFrame): DataFrame ={
    val transformer: Transformer = pipelineModel.stages(0)
    val indexerModel: StringIndexerModel = transformer.asInstanceOf[StringIndexerModel]
    val labels: Array[String] = indexerModel.labels   // 数组下标对应矢量值   下标位置的值是原值

    val indexToString = new IndexToString()
    indexToString.setLabels(labels)
    indexToString.setInputCol("prediction")
    indexToString.setOutputCol("prediction_origin")


    val convertedDataFrame: DataFrame = indexToString.transform(predictedDataFrame)
    convertedDataFrame
  }





}
