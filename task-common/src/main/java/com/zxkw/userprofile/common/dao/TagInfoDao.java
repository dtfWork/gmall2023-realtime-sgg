package com.zxkw.userprofile.common.dao;

import com.zxkw.userprofile.common.bean.TagInfo;
import com.zxkw.userprofile.common.util.MySqlUtil;

import java.util.List;

public class TagInfoDao {
    public static TagInfo getTagInfoByTaskId(String taskId){
        String sql = "select " +
                "id, tag_code, tag_name, tag_level, parent_tag_id , tag_type,tag_value_type, " +
                "tag_value_limit , tag_value_step , tag_task_id, tag_comment, update_time,create_time " +
                "from tag_info " +
                "where tag_task_id = " + taskId ;
        TagInfo tagInfo = MySqlUtil.queryOne(sql, TagInfo.class, true);

        return tagInfo ;
    }

    public static List<TagInfo> getTagInfoList(){
        String sql = " select ti.* " +
                " from tag_info ti join task_info tk " +
                " on ti.tag_task_id = tk.id " +
                " where tk.task_status = '1'";

        List<TagInfo> tagInfoList = MySqlUtil.queryList(sql, TagInfo.class, true);
        return tagInfoList;
    }
}
