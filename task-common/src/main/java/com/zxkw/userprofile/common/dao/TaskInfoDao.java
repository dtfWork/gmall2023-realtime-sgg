package com.zxkw.userprofile.common.dao;

import com.zxkw.userprofile.common.bean.TaskInfo;
import com.zxkw.userprofile.common.util.MySqlUtil;

public class TaskInfoDao {
    public static TaskInfo getTaskInfoById (String taskId){
        String sql =
                "select " +
                        " id ,task_name,task_status,task_comment,task_time,task_type,exec_type,main_class " +
                        " file_id, task_args, task_sql ,task_exec_level, create_time " +
                        " from task_info" +
                        " where id = " + taskId ;
        TaskInfo taskInfo = MySqlUtil.queryOne(sql, TaskInfo.class, true);
        return taskInfo ;
    }
}
