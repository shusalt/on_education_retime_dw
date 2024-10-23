package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficUjRate {

    // 来源
    String source_name;

    // 跳出会话数
    Double jump_session_count;
}
