package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficVisitorStatsPerHour {

    // 小时
    Integer hr;

    // 独立访客数
    Long uv_count;

    // 页面浏览数
    Long page_view_count;

    // 新访客数
    Long newUvCt;
}
