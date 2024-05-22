package com.zxkw.userprofile.common.bean;

import lombok.Data;

@Data
public class TaskTagRule {

    private  Long id;

    private  Long tagId;

    private  Long taskId;

    private  String queryValue;

    private  Long subTagId;

    private  String subTagValue;
}
