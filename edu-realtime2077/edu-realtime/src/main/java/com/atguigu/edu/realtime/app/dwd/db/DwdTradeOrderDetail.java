package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2023-04-24 15:18
 */
public class DwdTradeOrderDetail {
    public static void main(String[] args) {
        //TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2 设置表格TTL
        EnvUtil.setTableEnvStateTtl(tableEnv,"10s");

        //TODO 3 从kafka读取业务数据topic_db
        String groupId = "dwd_trade_order_detail";
        KafkaUtil.createTopicDb(tableEnv,groupId);

        //TODO 4 从kafka读取日志数据dwd_traffic_page_log
        tableEnv.executeSql("create table page_log(\n" +
                "    common map<String,String>,\n" +
                "    page map<String,String>,\n" +
                "    ts string\n" +
                ")" + KafkaUtil.getKafkaDDL("dwd_traffic_page_log",groupId));

        //TODO 5 过滤订单详情表
        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "    data['id'] id,\n" +
                "    data['course_id'] course_id,\n" +
                "    data['course_name'] course_name,\n" +
                "    data['order_id'] order_id,\n" +
                "    data['user_id'] user_id,\n" +
                "    data['origin_amount'] origin_amount,\n" +
                "    data['coupon_reduce'] coupon_reduce,\n" +
                "    data['final_amount'] final_amount,\n" +
                "    data['create_time'] create_time,\n" +
                "    date_format(data['create_time'],'yyyy-MM-dd') create_date,\n" +
                "    ts\n" +
                "from topic_db\n" +
                "where `table`='order_detail'\n" +
                "and type='insert'");
        tableEnv.createTemporaryView("order_detail",orderDetail);

        //TODO 6 过滤订单表
        Table orderInfo = tableEnv.sqlQuery("select \n" +
                "    data['id'] id, \n" +
                "    data['out_trade_no'] out_trade_no, \n" +
                "    data['trade_body'] trade_body, \n" +
                "    data['session_id'] session_id, \n" +
                "    data['province_id'] province_id\n" +
                "from topic_db\n" +
                "where `table`='order_info'\n" +
                "and type='insert'");
        tableEnv.createTemporaryView("order_info",orderInfo);

        //TODO 7 获取下单日志
        Table orderLog = tableEnv.sqlQuery("select \n" +
                "    common['sid'] session_id,\n" +
                "    common['sc'] source_id\n" +
                "from page_log\n" +
                "where page['page_id']='order'");
        tableEnv.createTemporaryView("order_log",orderLog);

        //TODO 8 关联3张表格
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "    od.id,\n" +
                "    od.course_id,\n" +
                "    od.course_name,\n" +
                "    od.order_id,\n" +
                "    od.user_id,\n" +
                "    od.origin_amount,\n" +
                "    od.coupon_reduce,\n" +
                "    od.final_amount,\n" +
                "    od.create_time,\n" +
                "    oi.out_trade_no,\n" +
                "    oi.trade_body,\n" +
                "    oi.session_id,\n" +
                "    oi.province_id,\n" +
                "    ol.source_id,\n" +
                "    ts,\n" +
                "    current_row_timestamp() row_op_ts \n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id=oi.id\n" +
                "left join order_log ol\n" +
                "on oi.session_id=ol.session_id");
        tableEnv.createTemporaryView("result_table",resultTable);
        //TODO 9 创建upsert kafka
        tableEnv.executeSql("create table dwd_trade_order_detail( \n" +
                "    id string,\n" +
                "    course_id string,\n" +
                "    course_name string,\n" +
                "    order_id string,\n" +
                "    user_id string,\n" +
                "    origin_amount string,\n" +
                "    coupon_reduce string,\n" +
                "    final_amount string,\n" +
                "    create_time string,\n" +
                "    out_trade_no string,\n" +
                "    trade_body string,\n" +
                "    session_id string,\n" +
                "    province_id string,\n" +
                "    source_id string,\n" +
                "    ts string,\n" +
                "    row_op_ts TIMESTAMP_LTZ(3) ,\n" +
                "    primary key(id) not enforced \n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_order_detail"));

        //TODO 10 写出数据到kafka
        tableEnv.executeSql("insert into dwd_trade_order_detail " +
                "select * from result_table");
    }
}
