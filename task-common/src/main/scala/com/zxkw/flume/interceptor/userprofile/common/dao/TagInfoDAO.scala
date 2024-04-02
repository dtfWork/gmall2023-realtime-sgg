package com.zxkw.flume.interceptor.userprofile.common.dao

import com.zxkw.flume.interceptor.userprofile.common.bean.TagInfo
import com.zxkw.flume.interceptor.userprofile.common.util.MySqlUtil

object TagInfoDAO {

  def getTagInfoByTaskId(taskId:String): TagInfo ={
    val tagInfoSql: String =
      s"""select id,tag_code,tag_name,
         | parent_tag_id,tag_type,tag_value_type,
         | tag_value_limit,tag_task_id,tag_comment,
         | create_time
         | from tag_info
         | where tag_task_id=$taskId""".stripMargin
    val tagInfoOpt: Option[TagInfo] =
      MySqlUtil.queryOne(tagInfoSql, classOf[TagInfo], true)
    var tagInfo: TagInfo = null;
    if (tagInfoOpt != None) {
      tagInfo = tagInfoOpt.get
    } else {
      throw new RuntimeException("no tag  for task_id  : "+taskId)
    }
    tagInfo
  }


  def  getTagInfoListTaskOn(): List[TagInfo] ={
    val tagSql=
      s"""
         | select tg.id,tag_code,tag_name,parent_tag_id,tag_type,tag_value_type,tag_value_limit,tag_task_id,tag_comment,tg.create_time    from
         | tag_info tg  , task_info tk
         |where tg.tag_task_id= tk.id
         | and  task_status='1'
         |
       """.stripMargin
    val tagInfoList: List[TagInfo] = MySqlUtil.queryList(tagSql, classOf[TagInfo],true)
     tagInfoList
  }



}
