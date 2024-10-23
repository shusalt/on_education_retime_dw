package com.atguigu.edu.publisher.service;

import com.atguigu.edu.publisher.bean.TrafficKeywords;

import java.util.List;

public interface TrafficKeywordsService {
    List<TrafficKeywords> getKeywords(Integer date);
}
