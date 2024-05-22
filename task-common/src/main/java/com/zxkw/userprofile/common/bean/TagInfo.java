package com.zxkw.userprofile.common.bean;

import lombok.Data;

import java.util.Date;

@Data
public class TagInfo {

    private  Long id;

    private  String tagCode;

    private  String tagName;

    private  Long tagLevel;

    private  Long parentTagId;

    private  String tagType;

    private  String tagValueType;

    private  Long tagTaskId;

    private  String tagComment;

    private Date createTime;

    private  Date updateTime;
}
