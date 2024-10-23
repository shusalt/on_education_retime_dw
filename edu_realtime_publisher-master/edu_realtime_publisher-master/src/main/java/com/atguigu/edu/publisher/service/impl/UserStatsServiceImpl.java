package com.atguigu.edu.publisher.service.impl;

import com.atguigu.edu.publisher.bean.UserChangeCtPerType;
import com.atguigu.edu.publisher.mapper.UserStatsMapper;
import com.atguigu.edu.publisher.service.UserStatsService;
import com.atguigu.edu.publisher.bean.UserPageCt;
import com.atguigu.edu.publisher.bean.UserTradeCt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserStatsServiceImpl  implements UserStatsService {

    @Autowired
    UserStatsMapper userStatsMapper;

    @Override
    public List<UserChangeCtPerType> getUserChangeCt(Integer date) {
        return userStatsMapper.selectUserChangeCtPerType(date);
    }

    @Override
    public List<UserPageCt> getUvByPage(Integer date) {
        return userStatsMapper.selectUvByPage(date);
    }

    @Override
    public List<UserTradeCt> getTradeUserCt(Integer date) {
        return userStatsMapper.selectTradeUserCt(date);
    }


}
