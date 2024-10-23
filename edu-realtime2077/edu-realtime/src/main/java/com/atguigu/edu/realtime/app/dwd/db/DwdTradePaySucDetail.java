package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author yhm
 * @create 2023-04-24 16:43
 */
public class DwdTradePaySucDetail {
    public static void main(String[] args) {
        //TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2 设置表格TTL
        EnvUtil.setTableEnvStateTtl(tableEnv,(15*60+5) + "s");

        //TODO 3 读取topic_db数据
        String groupId = "dwd_trade_pay_suc_detail";
        KafkaUtil.createTopicDb(tableEnv,groupId);
        //添加时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        //TODO 4 读取DwdTradeOrderDetail下单详情数据
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
                "    row_op_ts TIMESTAMP_LTZ(3) \n" +
                ")" + KafkaUtil.getKafkaDDL("dwd_trade_order_detail",groupId));

        //TODO 5 过滤支付成功数据
        Table payTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['alipay_trade_no'] alipay_trade_no,\n" +
                "    `data`['payment_type'] payment_type,\n" +
                "    `data`['payment_status'] payment_status,\n" +
                "    `data`['callback_content'] callback_content,\n" +
                "    `data`['callback_time'] callback_time,\n" +
                "    ts\n" +
                "from topic_db\n" +
                "where " +
//                "`type`='update'\n" +
                "`table`='payment_info'\n" +
                "and `data`['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_suc",payTable);

        //TODO 6 关联两张表格
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "    tod.id,\n" +
                "    tod.course_id,\n" +
                "    tod.course_name,\n" +
                "    tod.order_id,\n" +
                "    tod.user_id,\n" +
                "    tod.origin_amount,\n" +
                "    tod.coupon_reduce,\n" +
                "    tod.final_amount,\n" +
                "    tod.create_time,\n" +
                "    tod.out_trade_no,\n" +
                "    tod.trade_body,\n" +
                "    tod.session_id,\n" +
                "    tod.province_id,\n" +
                "    tod.source_id,\n" +
                "    ps.alipay_trade_no,\n" +
                "    ps.payment_type,\n" +
                "    ps.payment_status,\n" +
                "    ps.callback_content,\n" +
                "    ps.callback_time,\n" +
                "    ps.ts,\n" +
                "    tod.row_op_ts\n" +
                "from dwd_trade_order_detail tod\n" +
                "join payment_suc ps\n" +
                "on tod.order_id=ps.order_id");
        tableEnv.createTemporaryView("result_table",resultTable);

        //TODO 7 创建upsert Kafka 主题Kafka dwd_trade_pay_suc_detail
        tableEnv.executeSql("create table dwd_trade_pay_suc_detail(\n" +
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
                "    alipay_trade_no string,\n" +
                "    payment_type string,\n" +
                "    payment_status string,\n" +
                "    callback_content string,\n" +
                "    callback_time string,\n" +
                "    ts string,\n" +
                "    row_op_ts TIMESTAMP_LTZ(3) ,\n" +
                "    primary key(id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_suc_detail"));

        //TODO 8 写出到kafka
        tableEnv.executeSql("insert into dwd_trade_pay_suc_detail " +
                "select * from result_table");
    }
}
