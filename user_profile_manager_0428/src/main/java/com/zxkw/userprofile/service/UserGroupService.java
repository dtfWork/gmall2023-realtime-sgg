package com.zxkw.userprofile.service;

import com.zxkw.userprofile.bean.UserGroup;
import com.baomidou.mybatisplus.extension.service.IService;

public interface UserGroupService  extends IService<UserGroup> {

     public  void  saveUserGroupInfo(UserGroup userGroup);

    public  void genUserGroupUids( UserGroup userGroup);

    public void genUserGroupRedis(UserGroup userGroup);
}
