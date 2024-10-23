package com.atguigu.edu.publisher.service.impl;

import com.atguigu.edu.publisher.bean.*;
import com.atguigu.edu.publisher.mapper.TradeStatsMapper;
import com.atguigu.edu.publisher.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class TradeStatsServiceImpl  implements TradeStatsService {

    @Autowired
    private TradeStatsMapper tradeStatsMapper;

    @Override
    public List<TradeSourceStats> getTradeSourceStats(Integer date) {
        return tradeStatsMapper.selectTradeSourceStats(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date) {
        return tradeStatsMapper.selectTradeProvinceOrderAmount(date);
    }

    @Override
    public List<TradeProvinceOrderUuCount> getTradeProvinceOrderUuCount(Integer date) {
        return tradeStatsMapper.selectTradeProvinceOrderUuCount(date);
    }

    @Override
    public List<TradeProvinceOrderCount> getTradeProvinceOrderCount(Integer date) {
        return tradeStatsMapper.selectTradeProvinceOrderCount(date);
    }

    @Override
    public BigDecimal getOrderTotalAmount(Integer date) {
        return tradeStatsMapper.selectOrderTotalAmount(date);
    }

    @Override
    public Long getOrderTotalCount(Integer date) {
        return tradeStatsMapper.selectOrderTotalCt(date);
    }
}
