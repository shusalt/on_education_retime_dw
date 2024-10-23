package com.atguigu.edu.publisher.service;

import com.atguigu.edu.publisher.bean.ExaminationCourseStats;
import com.atguigu.edu.publisher.bean.ExaminationPaperStats;
import com.atguigu.edu.publisher.bean.ExaminationQuestionStats;
import com.atguigu.edu.publisher.bean.ExaminationScoreDurDistribution;

import java.util.List;

public interface ExaminationStatsService {
    List<ExaminationPaperStats> getExaminationPaperStats(Integer date);

    List<ExaminationCourseStats> getExaminationCourseStats(Integer date);

    List<ExaminationScoreDurDistribution> getExaminationScoreDurDistribution(Integer date);

    List<ExaminationQuestionStats> getExaminationQuestionStats(Integer date);
}
