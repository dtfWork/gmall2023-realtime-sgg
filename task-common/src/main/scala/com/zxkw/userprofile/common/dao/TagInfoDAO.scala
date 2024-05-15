package com.zxkw.userprofile.common.dao

import com.zxkw.userprofile.common.bean.TagInfo
import com.zxkw.userprofile.common.util.MySqlUtil

object TagInfoDAO {
  /**
   * 通过taskId得到标签信息
   * @param taskId
   * @return
   */
  // 定义一个名为getTagInfoByTaskId的函数，该函数以taskId作为String参数，并返回一个TagInfo对象。
  def getTagInfoByTaskId(taskId: String): TagInfo = {

    // 构建一个SQL查询，从tag_info表中选择特定的列，其中tag_task_id与给定的taskId匹配。
    val tagInfoSql: String =
      s"""select id, tag_code, tag_name,
         | parent_tag_id, tag_type, tag_value_type,
         | tag_value_limit, tag_task_id, tag_comment,
         | create_time
         | from tag_info
         | where tag_task_id=$taskId""".stripMargin

    // 使用构建的SQL查询查询数据库，并返回一个TagInfo的Option。
    val tagInfoOpt: Option[TagInfo] =
      MySqlUtil.queryOne(tagInfoSql, classOf[TagInfo], true)

    // 初始化一个类型为TagInfo的变量tagInfo。
    var tagInfo: TagInfo = null;

    // 检查tagInfoOpt是否包含一个值。
    if (tagInfoOpt != None) {
      // 如果是，则将该值赋给tagInfo。
      tagInfo = tagInfoOpt.get
    } else {
      // 如果否，则抛出一个带有指示给定taskId找不到标签的消息的RuntimeException。
      throw new RuntimeException("没有找到task_id为：" + taskId + "的标签")
    }

    // 返回tagInfo对象
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
