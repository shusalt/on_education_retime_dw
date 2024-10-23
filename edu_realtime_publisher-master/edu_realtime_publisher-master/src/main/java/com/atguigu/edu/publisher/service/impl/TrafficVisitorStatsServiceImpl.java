package com.atguigu.edu.publisher.service.impl;

import com.atguigu.edu.publisher.bean.TrafficVisitorTypeStats;
import com.atguigu.edu.publisher.mapper.TrafficVisitorStatsMapper;
import com.atguigu.edu.publisher.service.TrafficVisitorStatsService;
import com.atguigu.edu.publisher.bean.TrafficVisitorStatsPerHour;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service

public class TrafficVisitorStatsServiceImpl  implements TrafficVisitorStatsService {
    @Autowired

    private TrafficVisitorStatsMapper trafficVisitorStatsMapper;

    // 获取分时流量统计数据
    @Override
    public List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorStatsPerHr(date);
    }

    @Override
    public List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorTypeStats(date);
    }


}
