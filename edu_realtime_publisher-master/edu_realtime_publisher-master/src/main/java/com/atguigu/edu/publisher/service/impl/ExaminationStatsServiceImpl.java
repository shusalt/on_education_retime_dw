package com.atguigu.edu.publisher.service.impl;

import com.atguigu.edu.publisher.bean.ExaminationCourseStats;
import com.atguigu.edu.publisher.bean.ExaminationScoreDurDistribution;
import com.atguigu.edu.publisher.mapper.ExaminationStatsMapper;
import com.atguigu.edu.publisher.service.ExaminationStatsService;
import com.atguigu.edu.publisher.bean.ExaminationPaperStats;
import com.atguigu.edu.publisher.bean.ExaminationQuestionStats;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ExaminationStatsServiceImpl implements ExaminationStatsService {

    @Autowired
    ExaminationStatsMapper examinationStatsMapper;

    @Override
    public List<ExaminationPaperStats> getExaminationPaperStats(Integer date) {
        return examinationStatsMapper.selectExaminationPaperStats(date);
    }

    @Override
    public List<ExaminationCourseStats> getExaminationCourseStats(Integer date) {
        return examinationStatsMapper.selectExaminationCourseStats(date);
    }

    @Override
    public List<ExaminationScoreDurDistribution> getExaminationScoreDurDistribution(Integer date) {
        return examinationStatsMapper.selectExaminationScoreDurDistribution(date);
    }

    @Override
    public List<ExaminationQuestionStats> getExaminationQuestionStats(Integer date) {
        return examinationStatsMapper.selectExaminationQuestionStats(date);
    }
}
