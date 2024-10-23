package com.atguigu.edu.publisher.mapper;

import com.atguigu.edu.publisher.bean.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

public interface TradeStatsMapper {

    // 各来源交易统计
    @Select("select source_name,\n" +
            "       sum(order_total_amount) order_total_amount,\n" +
            "       sum(order_uu_count)  order_uu_ct,\n" +
            "       sum(order_count) order_ct\n" +
            "from dws_trade_source_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), source_id, source_name\n" +
            "order by order_total_amount desc\n" +
            "limit 10;")
    List<TradeSourceStats> selectTradeSourceStats(@Param("date") Integer date);

    // 各省份订单总额
    @Select("select province_name,\n" +
            "       sum(order_total_amount) order_total_amount\n" +
            "from dws_trade_province_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), province_id, province_name;")
    List<TradeProvinceOrderAmount> selectTradeProvinceOrderAmount(@Param("date") Integer date);

    // 各省份下单总人数
    @Select("select province_name,\n" +
            "       sum(order_uu_count)  order_uu_ct\n" +
            "from dws_trade_province_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), province_id, province_name;")
    List<TradeProvinceOrderUuCount> selectTradeProvinceOrderUuCount(@Param("date") Integer date);

    // 各省份订单总数
    @Select("select province_name,\n" +
            "       sum(order_count) order_ct\n" +
            "from dws_trade_province_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), province_id, province_name;")
    List<TradeProvinceOrderCount> selectTradeProvinceOrderCount(@Param("date") Integer date);

// 交易总金额
@Select("select sum(order_total_amount)  order_total_amount\n" +
        "from dws_trade_source_order_window\n" +
        "where toYYYYMMDD(stt) = #{date};")
BigDecimal selectOrderTotalAmount(@Param("date") Integer date);

// 订单总数
@Select("select sum(order_count)  order_total_count\n" +
        "from dws_trade_source_order_window\n" +
        "where toYYYYMMDD(stt) = #{date};")
Long selectOrderTotalCt(@Param("date") Integer date);
}
