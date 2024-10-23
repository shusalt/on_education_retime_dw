package com.atguigu.edu.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.DimUtil;
import com.atguigu.edu.realtime.util.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author yhm
 * @create 2023-04-20 16:29
 */
public class DimPhoenixSinkFunc implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        // TODO 1 获取输出的表名
        String sinkTable = jsonObject.getString("sink_table");
        String type = jsonObject.getString("type");
        String id = jsonObject.getString("id");
        jsonObject.remove("sink_table");
        jsonObject.remove("type");

        // TODO 2 使用工具类 写出数据
        PhoenixUtil.executeDML(sinkTable,jsonObject);

        // TODO 3 如果类型为update 删除redis对应缓存
        if ("update".equals(type)){
            DimUtil.deleteCached(sinkTable,id);
        }
    }
}
