package com.atguigu.edu.publisher.service;

import com.atguigu.edu.publisher.bean.UserChangeCtPerType;
import com.atguigu.edu.publisher.bean.UserPageCt;
import com.atguigu.edu.publisher.bean.UserTradeCt;

import java.util.List;

public interface UserStatsService {
    List<UserChangeCtPerType> getUserChangeCt(Integer date);
    List<UserPageCt> getUvByPage(Integer date);
    List<UserTradeCt> getTradeUserCt(Integer date);
}
