package com.atguigu.edu.realtime.bean;

import lombok.Data;

/**
 * @author yhm
 * @create 2023-04-20 11:10
 */
@Data
public class DimTableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
