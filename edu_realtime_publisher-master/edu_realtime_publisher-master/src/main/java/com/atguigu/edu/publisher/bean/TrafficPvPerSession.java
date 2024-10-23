package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficPvPerSession {
    // 来源

    String source_name;

    // 各会话页面浏览数
    Double page_view_count;
}
