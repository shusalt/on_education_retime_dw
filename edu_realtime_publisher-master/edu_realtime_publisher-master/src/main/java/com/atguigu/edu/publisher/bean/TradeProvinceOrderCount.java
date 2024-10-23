package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TradeProvinceOrderCount {

    // 省份名称
    String provinceName;

    // 订单数
    Long orderCt;
}
