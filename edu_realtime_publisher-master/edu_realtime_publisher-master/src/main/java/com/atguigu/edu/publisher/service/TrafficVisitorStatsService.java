package com.atguigu.edu.publisher.service;

import com.atguigu.edu.publisher.bean.TrafficVisitorStatsPerHour;
import com.atguigu.edu.publisher.bean.TrafficVisitorTypeStats;

import java.util.List;

public interface TrafficVisitorStatsService {

    // 获取分时流量数据
    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date);

    // 获取新老访客流量数据
    List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date);

}
