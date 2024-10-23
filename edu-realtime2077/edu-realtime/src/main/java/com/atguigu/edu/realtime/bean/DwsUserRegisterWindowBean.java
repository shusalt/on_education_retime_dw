package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2023-05-01 16:45
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsUserRegisterWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 注册用户数
    Long registerCount;

    // 时间戳
    Long ts;
}

