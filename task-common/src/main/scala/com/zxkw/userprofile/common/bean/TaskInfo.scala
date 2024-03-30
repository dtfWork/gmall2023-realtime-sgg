package com.atguigu.userprofile.common.bean

import java.util.Date
import java.lang.Long
case class TaskInfo  (var id: Long = null ,
                      var taskName:String=null,
                      var taskStatus:String=null,
                      var taskComment:String=null,
                      var taskTime:String=null,
                      var taskType:String=null,
                      var execType:String=null,
                      var mainClass:String=null,
                      var fileId:Long= null,
                      var taskArgs:String=null,
                      var taskSql:String=null,
                      var taskExecLevel:Long =null,
                      var createTime:Date=null)  {

  //补充无参构造函数
  def this()  ={
    this(null,null,null,null,null,null,null,null,null,null,null,null,null)
  }
}

