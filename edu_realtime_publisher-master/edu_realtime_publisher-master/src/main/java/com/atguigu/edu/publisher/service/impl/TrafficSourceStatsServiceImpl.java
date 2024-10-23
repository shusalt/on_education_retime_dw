package com.atguigu.edu.publisher.service.impl;

import com.atguigu.edu.publisher.bean.*;
import com.atguigu.edu.publisher.mapper.TrafficSourceStatsMapper;
import com.atguigu.edu.publisher.service.TrafficSourceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficSourceStatsServiceImpl implements TrafficSourceStatsService {
    // 自动装载 Mapper 接口实现类
    @Autowired
    TrafficSourceStatsMapper trafficSourceStatsMapper;

    // 1. 获取各来源独立访客数
    @Override
    public List<TrafficUvCt> getUvCt(Integer date) {
        return trafficSourceStatsMapper.selectUvCt(date);
    }

    // 2. 获取各来源会话数
    @Override
    public List<TrafficSvCt> getSvCt(Integer date) {
        return trafficSourceStatsMapper.selectSvCt(date);
    }

    // 3. 获取各来源会话平均页面浏览数
    @Override
    public List<TrafficPvPerSession> getPvPerSession(Integer date) {
        return trafficSourceStatsMapper.selectPvPerSession(date);
    }

    // 4. 获取各来源会话平均页面访问时长
    @Override
    public List<TrafficDurPerSession> getDurPerSession(Integer date) {
        return trafficSourceStatsMapper.selectDurPerSession(date);
    }

    // 5. 获取各来源跳出率
    @Override
    public List<TrafficUjRate> getUjRate(Integer date) {
        return trafficSourceStatsMapper.selectUjRate(date);
    }

}
