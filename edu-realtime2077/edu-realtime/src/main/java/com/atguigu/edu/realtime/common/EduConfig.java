package com.atguigu.edu.realtime.common;

/**
 * @author yhm
 * @create 2023-04-20 15:17
 */
public class EduConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "EDU_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    // kafka连接地址
    public static final String KAFKA_BOOTSTRAPS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/edu_realtime";

}
