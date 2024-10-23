package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class Category0rderStats {

    // 类别名称
    String categoryName;

    // 订单总额
    BigDecimal orderTotalAmount;
}
