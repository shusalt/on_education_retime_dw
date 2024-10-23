package com.atguigu.edu.publisher.bean;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class SubjectOrderStats {

    // 科目名称
    private String subjectName;

    // 订单总额
    private BigDecimal orderTotalAmount;
}