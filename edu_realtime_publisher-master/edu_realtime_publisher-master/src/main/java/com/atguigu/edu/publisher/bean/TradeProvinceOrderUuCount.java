package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class TradeProvinceOrderUuCount {

    // 省份名称
    String provinceName;

    // 下单独立用户数
    Long orderUuCt;
}
