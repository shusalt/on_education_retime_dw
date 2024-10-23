package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2023-05-01 10:42
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsUserLoginWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 回流用户数
    Long backCount;

    // 独立用户数
    Long uvCount;

    // 时间戳
    Long ts;
}

