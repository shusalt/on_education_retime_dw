package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2023-04-23 17:36
 */
public class DwdUserUserRegister {
    public static void main(String[] args) {
        //TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2 设置表的TTL
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10L));
        EnvUtil.setTableEnvStateTtl(tableEnv,"10s");

        //TODO 3 读取topic_db的数据
        String groupId = "dwd_user_user_register2";
        KafkaUtil.createTopicDb(tableEnv,groupId);

//        tableEnv.executeSql("select * from topic_db").print();

        //TODO 4 读取page主题数据dwd_traffic_page_log
        tableEnv.executeSql("CREATE TABLE page_log (\n" +
                "  `common` map<string,string>,\n" +
                "  `page` string,\n" +
                "  `ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("dwd_traffic_page_log",groupId));

        //TODO 5 过滤用户表数据
        Table userRegister = tableEnv.sqlQuery("select \n" +
                "    data['id'] id,\n" +
                "    data['create_time'] create_time,\n" +
                "    date_format(data['create_time'],'yyyy-MM-dd') create_date,\n" +
                "    ts\n" +
                "from topic_db\n" +
                "where `table`='user_info'\n" +
                "and `type`='insert'" +
                "");
        tableEnv.createTemporaryView("user_register",userRegister);

        //TODO 6 过滤注册日志的维度信息
        Table dimLog = tableEnv.sqlQuery("select \n" +
                        "    common['uid'] user_id,\n" +
                        "    common['ch'] channel, \n" +
                        "    common['ar'] province_id, \n" +
                        "    common['vc'] version_code, \n" +
                        "    common['sc'] source_id, \n" +
                        "    common['mid'] mid_id, \n" +
                        "    common['ba'] brand, \n" +
                        "    common['md'] model, \n" +
                        "    common['os'] operate_system \n" +

                        "from page_log\n" +
                        "where common['uid'] is not null \n"
                //"and page['page_id'] = 'register'"
        );
        tableEnv.createTemporaryView("dim_log",dimLog);



        //TODO 7 join两张表格
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "    ur.id user_id,\n" +
                "    create_time register_time,\n" +
                "    create_date register_date,\n" +
                "    channel,\n" +
                "    province_id,\n" +
                "    version_code,\n" +
                "    source_id,\n" +
                "    mid_id,\n" +
                "    brand,\n" +
                "    model,\n" +
                "    operate_system,\n" +
                "    ts, \n" +
                "    current_row_timestamp() row_op_ts \n" +
                "from user_register ur \n" +
                "left join dim_log pl \n" +
                "on ur.id=pl.user_id");

        tableEnv.createTemporaryView("result_table",resultTable);

        //TODO 8 写出数据到kafka
        tableEnv.executeSql(" create table dwd_user_user_register(\n" +
                "    user_id string,\n" +
                "    register_time string,\n" +
                "    register_date string,\n" +
                "    channel string,\n" +
                "    province_id string,\n" +
                "    version_code string,\n" +
                "    source_id string,\n" +
                "    mid_id string,\n" +
                "    brand string,\n" +
                "    model string,\n" +
                "    operate_system string,\n" +
                "    ts string,\n" +
                "    row_op_ts TIMESTAMP_LTZ(3) ,\n" +
                "    PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_user_user_register"));


        tableEnv.executeSql("insert into dwd_user_user_register " +
                "select * from result_table");

    }
}
