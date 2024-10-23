package com.atguigu.edu.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author yhm
 * @create 2023-04-30 20:20
 */
public interface DimJoinFunction<T> {
    // 关联方法
    void join(T obj, JSONObject jsonObject) throws Exception;

    // 获取关联字段
    String getKey(T obj);

}
