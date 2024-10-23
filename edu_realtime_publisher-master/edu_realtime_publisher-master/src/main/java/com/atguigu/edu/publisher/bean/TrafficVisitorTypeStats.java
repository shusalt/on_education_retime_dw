package com.atguigu.edu.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficVisitorTypeStats {

    // 新老访客状态标记
    String isNew;

    // 独立访客数
    Long uv_count;

    // 页面浏览数
    Long page_view_count;

    // 会话数
    Long total_session_count;

    // 跳出会话数
    Long jump_session_count;

    // 累计访问时长
    Long total_during_time;

    // 跳出率
    public Double getUjRate() {
        if (total_session_count == 0) {
            return 0.0;
        }
        return (double) jump_session_count / (double) total_session_count;
    }

    // 会话平均在线时长（秒）
    public Double getAvgDurSum() {
        if (total_session_count == 0) {
            return 0.0;
        }
        return (double) total_during_time / (double) total_session_count / 1000;
    }

    // 会话平均访问页面数
    public Double getAvgPvCt() {
        if (total_session_count == 0) {
            return 0.0;
        }
        return (double) page_view_count / (double) total_session_count;
    }
}
