package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CourseCommodityStats {
    // 课程名称
    String courseName;

    // 订单人数
    Integer uuCt;
    // 订单金额
    Double orderAmount;
}
