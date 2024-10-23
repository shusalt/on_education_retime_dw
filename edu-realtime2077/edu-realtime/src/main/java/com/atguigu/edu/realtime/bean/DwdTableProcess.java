package com.atguigu.edu.realtime.bean;

import lombok.Data;

/**
 * @author yhm
 * @create 2023-04-24 18:04
 */
@Data
public class DwdTableProcess {

    // 来源表
    String sourceTable;

    // 操作类型
    String sourceType;

    // 输出表
    String sinkTable;

    // 输出字段
    String sinkColumns;
}
