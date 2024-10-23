package com.atguigu.edu.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DimTableProcess;
import com.atguigu.edu.realtime.common.EduConfig;
import com.atguigu.edu.realtime.util.DruidDSUtil;
import com.atguigu.edu.realtime.util.PhoenixUtil;
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
public class DimBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {



    private MapStateDescriptor<String, DimTableProcess> tableProcessState;

    // 初始化配置表数据
    private HashMap<String, DimTableProcess> configMap = new HashMap<>();

    public DimBroadcastProcessFunction(MapStateDescriptor<String, DimTableProcess> tableProcessState) {
        this.tableProcessState = tableProcessState;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/edu_config?" +
                "user=root&password=123456&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false"
        );

        PreparedStatement preparedStatement = connection.prepareStatement("select * from edu_config.table_process");
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()){
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String columnValue = resultSet.getString(i);
                jsonObject.put(columnName,columnValue);
            }
            DimTableProcess dimTableProcess = jsonObject.toJavaObject(DimTableProcess.class);
            configMap.put(dimTableProcess.getSourceTable(),dimTableProcess);
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
        BroadcastState<String, DimTableProcess> tableConfigState = ctx.getBroadcastState(tableProcessState);
        if ("d".equals(type)){
            // 从状态中删除对应的表格
            DimTableProcess before = jsonObject.getObject("before", DimTableProcess.class);
            tableConfigState.remove(before.getSourceTable());
            // 从configMap中删除对应的表格
            configMap.remove(before.getSourceTable());
        }else{
            DimTableProcess after = jsonObject.getObject("after", DimTableProcess.class);

            //TODO 3 将数据写入到状态 广播出去
            tableConfigState.put(after.getSourceTable(),after);
            //TODO 2 检查phoenix中是否存在表  不存在创建
            String sinkTable = after.getSinkTable();
            String sinkColumns = after.getSinkColumns();
            String sinkPk = after.getSinkPk();
            String sinkExtend = after.getSinkExtend();
            checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
        }

    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // create table if not exists table (id string pk,name string...)
        // 拼接建表语句的sql
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + EduConfig.HBASE_SCHEMA + "." + sinkTable + "(\n");
        // 判断主键
        // 如果主键为空,默认使用id
        if (sinkPk==null){
            sinkPk="";
        }
        if (sinkExtend==null){
            sinkExtend="";
        }

        // 遍历字段拼接建表语句
        String[] split = sinkColumns.split(",");
        for (int i = 0; i < split.length; i++) {
            sql.append(split[i] + " varchar");
            if (split[i].equals(sinkPk)){
                sql.append(" primary key");
            }
            if (i < split.length - 1){
                sql.append(",\n");
            }
        }
        sql.append(") ");
        sql.append(sinkExtend);

        PhoenixUtil.executeDDL(sql.toString());

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
        ReadOnlyBroadcastState<String, DimTableProcess> tableConfigState = ctx.getBroadcastState(tableProcessState);
        DimTableProcess tableProcess = tableConfigState.get(value.getString("table"));
        // 补充情况,防止kafka数据到的过快  造成数据丢失
        if (tableProcess == null){
            tableProcess = configMap.get(value.getString("table"));
        }
        if (tableProcess != null){

            String type = value.getString("type");
            if (type == null){
                System.out.println("maxwell采集的数据不完整");
            }else{
                JSONObject data = value.getJSONObject("data");
                //TODO 2 过滤出需要的维度字段
                String sinkColumns = tableProcess.getSinkColumns();
                filterColumns(data,sinkColumns);
                //TODO 3 补充输出字段
                data.put("sink_table",tableProcess.getSinkTable());
                // 添加数据的类型
                data.put("type",type);

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
