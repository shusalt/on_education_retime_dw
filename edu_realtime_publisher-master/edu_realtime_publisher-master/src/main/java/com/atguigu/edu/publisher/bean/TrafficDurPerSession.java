package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficDurPerSession {

    // 来源
    String source_name;

    // 各会话页面停留时长
    Double total_during_sec;
}
