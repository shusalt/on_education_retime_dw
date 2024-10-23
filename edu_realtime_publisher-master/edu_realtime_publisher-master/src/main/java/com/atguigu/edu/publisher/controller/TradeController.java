package com.atguigu.edu.publisher.controller;

import com.atguigu.edu.publisher.bean.*;
import com.atguigu.edu.publisher.service.TradeStatsService;
import com.atguigu.edu.publisher.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/edu/realtime/trade")
public class TradeController {

    @Autowired
    private TradeStatsService tradeStatsService;

    @RequestMapping("/sourceStats")
    public String getStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {

        if (date == 1) {
            date = DateUtil.now();
        }

        List<TradeSourceStats> tradeSourceStatsList = tradeStatsService.getTradeSourceStats(date);

        if (tradeSourceStatsList == null || tradeSourceStatsList.size() == 0) {
            return "";
        }

        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < tradeSourceStatsList.size(); i++) {

            TradeSourceStats tradeSourceStats = tradeSourceStatsList.get(i);

            String sourceName = tradeSourceStats.getSourceName();
            BigDecimal orderTotalAmount = tradeSourceStats.getOrderTotalAmount();
            Long orderUuCt = tradeSourceStats.getOrderUuCt();
            Long orderCt = tradeSourceStats.getOrderCt();

            rows.append("{\n" +
                    "        \"source_name\": \"" + sourceName + "\",\n" +
                    "        \"order_total_amount\": \"" + orderTotalAmount + "\",\n" +
                    "        \"order_uu_count\": \"" + orderUuCt + "\",\n" +
                    "        \"order_count\": \"" + orderCt + "\"\n" +
                    "      }");

            if (i < tradeSourceStatsList.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"columns\": [\n" +
                "      {\n" +
                "        \"name\": \"来源名称\",\n" +
                "        \"id\": \"source_name\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单总额\",\n" +
                "        \"id\": \"order_total_amount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"下单用户总数\",\n" +
                "        \"id\": \"order_uu_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单总数\",\n" +
                "        \"id\": \"order_count\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": \n" + rows +
                "    \n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/provinceOrderAmount")
    public String getProvinceOrderAmount(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TradeProvinceOrderAmount> tradeProvinceOrderAmountList = tradeStatsService.getTradeProvinceOrderAmount(date);
        if (tradeProvinceOrderAmountList == null || tradeProvinceOrderAmountList.size() == 0) {
            return "";
        }
        StringBuilder mapData = new StringBuilder("[");
        for (int i = 0; i < tradeProvinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount tradeProvinceOrderAmount = tradeProvinceOrderAmountList.get(i);
            String provinceName = tradeProvinceOrderAmount.getProvinceName();
            BigDecimal orderAmount = tradeProvinceOrderAmount.getOrderTotalAmount();
            mapData.append("{\n" +
                    "        \"name\": \"" + provinceName + "\",\n" +
                    "        \"value\": " + orderAmount + "\n" +
                    "      }");
            if (i < tradeProvinceOrderAmountList.size() - 1) {
                mapData.append(",");
            } else {
                mapData.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"mapData\": " + mapData + ",\n" +
                "    \"valueName\": \"订单金额\"\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/provinceOrderUuCt")
    public String getProvinceOrderUuCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TradeProvinceOrderUuCount> tradeProvinceOrderUuCountList = tradeStatsService.getTradeProvinceOrderUuCount(date);
        if (tradeProvinceOrderUuCountList == null || tradeProvinceOrderUuCountList.size() == 0) {
            return "";
        }
        StringBuilder mapData = new StringBuilder("[");
        for (int i = 0; i < tradeProvinceOrderUuCountList.size(); i++) {
            TradeProvinceOrderUuCount tradeProvinceOrderUuCount = tradeProvinceOrderUuCountList.get(i);
            String provinceName = tradeProvinceOrderUuCount.getProvinceName();
            Long orderUuCt = tradeProvinceOrderUuCount.getOrderUuCt();
            mapData.append("{\n" +
                    "        \"name\": \"" + provinceName + "\",\n" +
                    "        \"value\": " + orderUuCt + "\n" +
                    "      }");
            if (i < tradeProvinceOrderUuCountList.size() - 1) {
                mapData.append(",");
            } else {
                mapData.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"mapData\": " + mapData + ",\n" +
                "    \"valueName\": \"下单独立用户数\"\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/provinceOrderCt")
    public String getProvinceOrderCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TradeProvinceOrderCount> tradeProvinceOrderCountList = tradeStatsService.getTradeProvinceOrderCount(date);
        if (tradeProvinceOrderCountList == null || tradeProvinceOrderCountList.size() == 0) {
            return "";
        }
        StringBuilder mapData = new StringBuilder("[");
        for (int i = 0; i < tradeProvinceOrderCountList.size(); i++) {
            TradeProvinceOrderCount tradeProvinceOrderCount = tradeProvinceOrderCountList.get(i);
            String provinceName = tradeProvinceOrderCount.getProvinceName();
            Long orderCt = tradeProvinceOrderCount.getOrderCt();
            mapData.append("{\n" +
                    "        \"name\": \"" + provinceName + "\",\n" +
                    "        \"value\": " + orderCt + "\n" +
                    "      }");
            if (i < tradeProvinceOrderCountList.size() - 1) {
                mapData.append(",");
            } else {
                mapData.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"mapData\": " + mapData + ",\n" +
                "    \"valueName\": \"订单数\"\n" +
                "  }\n" +
                "}";
    }

    // 订单总额
    @RequestMapping("/gmv")
    public String getTotalAmount(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        BigDecimal totalAmount = tradeStatsService.getOrderTotalAmount(date);
        if (totalAmount == null) {
            return "";
        }
        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": " + totalAmount + "\n" +
                "}";
    }

    // 订单总数
    @RequestMapping("/orderTotalCount")
    public String getOrderTotalCount(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        Long orderTotalCount = tradeStatsService.getOrderTotalCount(date);
        if (orderTotalCount == null) {
            return "";
        }
        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": " + orderTotalCount + "\n" +
                "}";
    }
}
