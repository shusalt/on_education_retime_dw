package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficSvCt {
    // 来源
    String source_name;
    // 会话数
    Long total_session_count;
}
