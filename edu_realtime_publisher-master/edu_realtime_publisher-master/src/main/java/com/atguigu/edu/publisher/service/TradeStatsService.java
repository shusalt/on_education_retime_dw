package com.atguigu.edu.publisher.service;

import com.atguigu.edu.publisher.bean.*;

import java.math.BigDecimal;
import java.util.List;

public interface TradeStatsService {

    List<TradeSourceStats> getTradeSourceStats(Integer date);

    List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date);

    List<TradeProvinceOrderUuCount> getTradeProvinceOrderUuCount(Integer date);

    List<TradeProvinceOrderCount> getTradeProvinceOrderCount(Integer date);

BigDecimal getOrderTotalAmount(Integer date);

Long  getOrderTotalCount(Integer date);
}
