package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {

    // 省份名称
    String provinceName;

    // 订单总额
    BigDecimal orderTotalAmount;
}
