package com.atguigu.edu.publisher.bean;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CourseReviewStats {

    // 课程名称
    String courseName;

    // 平均评分
    Double avgStars;

    // 评价用户数
    Integer reviewUserCt;

    // 好评用户数
    Integer goodReviewUserCt;

    // 好评率
    Double goodReviewRate;
}
