package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ExaminationCourseStats {

    // 课程名称
    String courseName;

    // 考试人次
    Long examTakenCount;

    // 试卷平均分
    Double avgScore;

    // 平均时长
    Double avgSec;
}
