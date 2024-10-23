package com.atguigu.edu.realtime.util;

import com.atguigu.edu.realtime.bean.KeywordBean;
import com.atguigu.edu.realtime.bean.TransientSink;
import com.atguigu.edu.realtime.common.EduConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author yhm
 * @create 2023-04-25 18:23
 */
public class ClickHouseUtil {
    // 设计泛型 通过传入的数据类型自动补充sql 写出到clickhouse
    public static <T> SinkFunction<T> getJdbcSink(String sql){
         return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        // T是泛型  明文是不知道什么类型的  需要使用反射获取
                        Field[] declaredFields = obj.getClass().getDeclaredFields();
                        int skip=0;
                        for (int i = 0; i < declaredFields.length; i++) {

                            Field field = declaredFields[i];
                            field.setAccessible(true);

                            // 获取属性的注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null){
                                skip++;
                                continue;
                            }

                            // 使用类模板的属性名  get对象  获取值
                            try {
                                Object o = field.get(obj);
                                preparedStatement.setObject(i + 1 - skip,o);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }, JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(5000L)
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(EduConfig.CLICKHOUSE_URL)
                        .withDriverName(EduConfig.CLICKHOUSE_DRIVER)
                        .build());
    }
}
