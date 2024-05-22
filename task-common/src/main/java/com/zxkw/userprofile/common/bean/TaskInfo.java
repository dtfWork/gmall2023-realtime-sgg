package com.zxkw.userprofile.common.bean;

import lombok.Data;

import java.util.Date;

@Data
public class TaskInfo {
    private  Long id;

    private  String taskName;

    private  String taskStatus;

    private  String taskComment;

    private  String taskType;

    private  String execType;

    private  String mainClass;

    private  Long fileId;

    private  String taskArgs;

    private  String taskSql;

    private  Long taskExecLevel;

    private Date createTime;
}
