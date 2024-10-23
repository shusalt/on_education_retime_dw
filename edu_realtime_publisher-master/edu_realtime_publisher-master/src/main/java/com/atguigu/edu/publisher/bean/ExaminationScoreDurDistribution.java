package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ExaminationScoreDurDistribution {

    // 分数段
    String scoreDuration;

    // 人数
    Long userCout;
}
