package com.zxkw.userprofile.controller;


import com.alibaba.fastjson.JSONObject;
import com.zxkw.userprofile.bean.UserGroup;
import com.zxkw.userprofile.service.UserGroupService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * <p>
 *  用户分群:前端控制器
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */
@RestController
public class UserGroupController {

    @Autowired
    UserGroupService userGroupService;

    @RequestMapping("/user-group-list")
    @CrossOrigin
    public String  getUserGroupList(@RequestParam("pageNo")int pageNo , @RequestParam("pageSize") int pageSize){
        //
        int startNo=(  pageNo-1)* pageSize;
        int endNo=startNo+pageSize;

        QueryWrapper<UserGroup> queryWrapper = new QueryWrapper<>();
        int count = userGroupService.count(queryWrapper);

        queryWrapper.orderByDesc("id").last(" limit " + startNo + "," + endNo);
        List<UserGroup> userGroupList = userGroupService.list(queryWrapper);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("detail",userGroupList);
        jsonObject.put("total",count);

        return  jsonObject.toJSONString();
    }

//接收请求
//调用服务层
//返回结果
    @PostMapping("/user-group")
    public  String  saveUserGroup(@RequestBody  UserGroup userGroup){

       //保存基本信息
       userGroupService.saveUserGroupInfo(userGroup);
     //保存人群包
        userGroupService.genUserGroupUids(   userGroup);
      //保存人群包到redis
        userGroupService.genUserGroupRedis(  userGroup);

        //throw  new RuntimeException("测试异常");
        return  "success";

    }







}

