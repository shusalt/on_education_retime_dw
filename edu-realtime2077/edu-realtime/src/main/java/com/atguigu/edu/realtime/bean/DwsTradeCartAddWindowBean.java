package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2023-05-01 20:12
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTradeCartAddWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 加购独立用户数
    Long cartAddUvCount;

    // 时间戳
    Long ts;
}
