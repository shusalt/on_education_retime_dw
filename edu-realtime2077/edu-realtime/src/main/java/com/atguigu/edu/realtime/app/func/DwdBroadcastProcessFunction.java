package com.atguigu.edu.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DimTableProcess;
import com.atguigu.edu.realtime.bean.DwdTableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * @author yhm
 * @create 2023-04-20 11:15
 */
public class DwdBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {



    private MapStateDescriptor<String, DwdTableProcess> tableProcessState;

    // 初始化配置表数据
    private HashMap<String, DwdTableProcess> configMap = new HashMap<>();

    public DwdBroadcastProcessFunction(MapStateDescriptor<String, DwdTableProcess> tableProcessState) {
        this.tableProcessState = tableProcessState;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/edu_config?" +
                "user=root&password=123456&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false"
        );

        PreparedStatement preparedStatement = connection.prepareStatement("select * from edu_config.dwd_table_process");
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()){
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String columnValue = resultSet.getString(i);
                jsonObject.put(columnName,columnValue);
            }
            DwdTableProcess dwdTableProcess = jsonObject.toJavaObject(DwdTableProcess.class);
            configMap.put(dwdTableProcess.getSourceTable(),dwdTableProcess);
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }

    /**
     *
     * @param value flinkCDC直接输入的json
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //TODO 1 获取配置表数据解析格式
        JSONObject jsonObject = JSON.parseObject(value);
        String type = jsonObject.getString("op");
        BroadcastState<String, DwdTableProcess> tableConfigState = ctx.getBroadcastState(tableProcessState);
        if ("d".equals(type)){
            // 从状态中删除对应的表格
            DwdTableProcess before = jsonObject.getObject("before", DwdTableProcess.class);
            tableConfigState.remove(before.getSourceTable());
            // 从configMap中删除对应的表格
            configMap.remove(before.getSourceTable());
        }else{
            DwdTableProcess after = jsonObject.getObject("after", DwdTableProcess.class);

            //TODO 2 将数据写入到状态 广播出去
            tableConfigState.put(after.getSourceTable(),after);

        }

    }


    /**
     *
     * @param value kafka中maxwell生成的json数据
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //TODO 1 获取广播的配置数据
        ReadOnlyBroadcastState<String, DwdTableProcess> tableConfigState = ctx.getBroadcastState(tableProcessState);
        DwdTableProcess tableProcess = tableConfigState.get(value.getString("table"));
        // 补充情况,防止kafka数据到的过快  造成数据丢失
        if (tableProcess == null){
            tableProcess = configMap.get(value.getString("table"));
        }
        if (tableProcess != null){

            String type = value.getString("type");
            if (type == null){
                System.out.println("maxwell采集的数据不完整");
            }else if (type.equals(tableProcess.getSourceType())){
                JSONObject data = value.getJSONObject("data");
                //TODO 2 过滤出需要的维度字段
                String sinkColumns = tableProcess.getSinkColumns();
                filterColumns(data,sinkColumns);
                //TODO 3 补充输出字段
                data.put("sink_table",tableProcess.getSinkTable());
                data.put("ts",value.getLong("ts"));

                out.collect(data);
            }
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> stringList = Arrays.asList(sinkColumns.split(","));
        entries.removeIf(entry -> !stringList.contains(entry.getKey()));
    }
}
