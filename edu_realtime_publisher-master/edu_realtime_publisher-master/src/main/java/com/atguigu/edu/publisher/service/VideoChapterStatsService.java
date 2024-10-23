package com.atguigu.edu.publisher.service;

import com.atguigu.edu.publisher.bean.VedioChapterStats;

import java.util.List;

public interface VideoChapterStatsService {
    List<VedioChapterStats> getVedioChapterStats(Integer date);
}
