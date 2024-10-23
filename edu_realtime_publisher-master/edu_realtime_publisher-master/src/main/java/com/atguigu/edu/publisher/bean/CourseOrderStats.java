package com.atguigu.edu.publisher.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CourseOrderStats {

    // 课程名称
    String courseName;

    // 订单总额
    BigDecimal orderTotalAmount;
}

