package com.atguigu.edu.publisher.service.impl;

import com.atguigu.edu.publisher.bean.VedioChapterStats;
import com.atguigu.edu.publisher.mapper.VideoChapterStatsMapper;
import com.atguigu.edu.publisher.service.VideoChapterStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class VideoChapterStatsServiceImpl implements VideoChapterStatsService {
    @Autowired
    VideoChapterStatsMapper videoChapterStatsMapper;

    @Override
    public List<VedioChapterStats> getVedioChapterStats(Integer date) {
        return videoChapterStatsMapper.selectVedioChapterStats(date);
    }
}
