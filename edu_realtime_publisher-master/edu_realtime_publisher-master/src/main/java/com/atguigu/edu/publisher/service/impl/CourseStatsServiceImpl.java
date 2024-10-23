package com.atguigu.edu.publisher.service.impl;

import com.atguigu.edu.publisher.bean.*;
import com.atguigu.edu.publisher.mapper.CourseStatsMapper;
import com.atguigu.edu.publisher.service.CourseStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
public class CourseStatsServiceImpl implements CourseStatsService {

    @Autowired
    private CourseStatsMapper courseStatsMapper;

    @Override
    public List<CourseOrderStats> getCourseOrderStats(Integer date) {
        return courseStatsMapper.selectCourseOrderStats(date);
    }

    @Override
    public List<SubjectOrderStats> getSubjectOrderStats(Integer date) {
        return courseStatsMapper.selectSubjectOrderStats(date);
    }

    @Override
    public List<Category0rderStats> getCategoryOrderStats(Integer date) {
        return courseStatsMapper.selectCategoryOrderStats(date);
    }

    @Override
    public List<CourseReviewStats> getCourseReviewStats(Integer date) {
        return courseStatsMapper.selectCourseReviewStats(date);
    }
}
