package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ExaminationPaperStats {

    // 试卷名称
    String paperTitle;

    // 考试人次
    Long examTakenCount;

    // 试卷平均分
    Double avgScore;

    // 平均时长
    Double avgSec;
}
