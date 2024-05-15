package com.zxkw.userprofile.app

import com.zxkw.userprofile.common.bean.TagInfo
import com.zxkw.userprofile.common.constants.ConstCode
import com.zxkw.userprofile.common.dao.TagInfoDAO
import com.zxkw.userprofile.common.util.ClickhouseUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TaskBitmapApp {

  def main(args: Array[String]): Unit = {


    //  insert select   在哪执行？ clickhouse执行
    //
    // insert into  user_tag_value_string   // 要区分 哪些标签写入该表
    //   select  tagv.tag.1, tagv.tag.2 ,  groupBitmapState(uid)，$taskDate
    //  from
    //  (
    // select uid, arrayJoin(  [('gender',gender),('agegroup',agegroup),('favor',favor)]) tag // 要区分 哪些标签写入该表
    // from user_tag_merge_$taskDate
    // )  tagv
    // group by tagv.tag.1, tagv.tag.2

    val sparkConf: SparkConf = new SparkConf().setAppName("task_bitmap_app")//.setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)


    val taskId: String = args(0)
    val taskDate: String = args(1)

    // 1  要把所有的标签都先取出来
    // 2  分类  分成4类  分别插入到4个表里
    // 3   把写入表操作放到方法里


    // 1  要把所有的标签都先取出来
     val tagAllList: List[TagInfo] = TagInfoDAO.getTagInfoListTaskOn()
    // 2  分类  分成4类  分别插入到4个表里

    val tagStringList: ListBuffer[TagInfo]=new ListBuffer()
    val tagLongList: ListBuffer[TagInfo]=new ListBuffer()
    val tagDecimalList: ListBuffer[TagInfo]=new ListBuffer()
    val tagDateList: ListBuffer[TagInfo]=new ListBuffer()

    for (tagInfo <- tagAllList ) {
      if(tagInfo.tagValueType==ConstCode.TAG_VALUE_TYPE_STRING){
        tagStringList.append(tagInfo)
      }else if(tagInfo.tagValueType==ConstCode.TAG_VALUE_TYPE_LONG){
        tagLongList.append(tagInfo)
      }else if(tagInfo.tagValueType==ConstCode.TAG_VALUE_TYPE_DECIMAL){
        tagDecimalList.append(tagInfo)
      }else if(tagInfo.tagValueType==ConstCode.TAG_VALUE_TYPE_DATE){
        tagDateList.append(tagInfo)
      }
    }

    insertBitmap( tagStringList,"user_tag_value_string",taskDate:String)
    insertBitmap( tagLongList,"user_tag_value_long",taskDate:String)
    insertBitmap( tagDecimalList,"user_tag_value_decimal",taskDate:String)
    insertBitmap( tagDateList,"user_tag_value_date",taskDate:String)




  }

//  根据不同的标签值 插入到不同的表
  def insertBitmap(tagInfoList:ListBuffer[TagInfo],targetTable:String,taskDate:String): Unit ={
    if(tagInfoList.size>0){
    //('gender',gender),('agegroup',agegroup),('favor',favor)
        val tagCodeList: List[String] = tagInfoList.toList.map(tagInfo=> s"('${tagInfo.tagCode}', ${tagInfo.tagCode.toLowerCase} )")
        val tagCodeSQL: String = tagCodeList.mkString(",")
        val deleteSQL:String =
          s"""
             | alter table $targetTable  delete where dt='$taskDate'
             |
           """.stripMargin
         println(deleteSQL)
         ClickhouseUtil.executeSql(deleteSQL)



        val  insertSQL=
          s"""
             |    insert into  $targetTable
             |        select  tagv.tag.1, tagv.tag.2 ,  groupBitmapState(cast( uid as UInt64)),'$taskDate'
             |      from
             |      (
             |     select uid, arrayJoin(  [$tagCodeSQL]) tag
             |     from user_tag_merge_${taskDate.replace("-","")}
             |     )  tagv
             |     where  tagv.tag.2<>''
             |     group by tagv.tag.1, tagv.tag.2
           """.stripMargin
        println(insertSQL)
        ClickhouseUtil.executeSql(insertSQL)  //driver

    }

  }

}
