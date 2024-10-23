package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class TradeSourceStats {

    // 来源名称
    String SourceName;

    // 订单总额
    BigDecimal orderTotalAmount;

    // 下单独立用户数
    Long orderUuCt;

    // 订单数
    Long orderCt;
}
