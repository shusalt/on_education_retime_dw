package com.atguigu.edu.publisher.service.impl;

import com.atguigu.edu.publisher.mapper.TrafficKeywordsMapper;
import com.atguigu.edu.publisher.service.TrafficKeywordsService;
import com.atguigu.edu.publisher.bean.TrafficKeywords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficKeywordsServiceImpl implements TrafficKeywordsService {

    @Autowired
    TrafficKeywordsMapper trafficKeywordsMapper;

    @Override
    public List<TrafficKeywords> getKeywords(Integer date) {
        return trafficKeywordsMapper.selectKeywords(date);
    }
}
