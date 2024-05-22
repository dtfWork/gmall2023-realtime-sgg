package com.zxkw.userprofile.common.dao;

import com.zxkw.userprofile.common.bean.TaskTagRule;
import com.zxkw.userprofile.common.util.MySqlUtil;

import java.util.List;

public class TaskTagRuleDao {
    public static List<TaskTagRule> getTaskTagRuleList(String taskId){
        String sql =
                " select tr.id , tr.tag_id, tr.task_id ,tr.query_value ,tr.sub_tag_id , ti.tag_name as sub_tag_value " +
                        " from task_tag_rule tr join tag_info ti " +
                        " on tr.sub_tag_id = ti.id " +
                        " where task_id = " + taskId ;
        List<TaskTagRule> taskTagRules = MySqlUtil.queryList(sql, TaskTagRule.class, true);
        return taskTagRules ;
    }
}
