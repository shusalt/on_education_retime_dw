package com.atguigu.edu.realtime.app.dws;

import com.atguigu.edu.realtime.app.func.KeyWordUDTF;
import com.atguigu.edu.realtime.bean.KeywordBean;
import com.atguigu.edu.realtime.common.EduConfig;
import com.atguigu.edu.realtime.common.EduConstant;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author yhm
 * @create 2023-04-25 16:01
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2 自定义拆词函数
        tableEnv.createTemporarySystemFunction("ik_analyze", new KeyWordUDTF());

        //TODO 3 读取kafka中的page_log数据
        String topicName = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("create table page_log(\n" +
                "    common map<String,String>,\n" +
                "    page map<String,String>,\n" +
                "    ts bigint, \n" +
                "    row_time as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')), \n" +
                "    WATERMARK FOR row_time AS row_time - INTERVAL '3' SECOND" +
                ")" + KafkaUtil.getKafkaDDL(topicName, groupId));

        //TODO 4 过滤数据得到搜索的关键字
        //① page 字段下 item 字段不为 null；
        //② page 字段下 last_page_id 为 search；
        //③ page 字段下 item_type 为 keyword。
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "    page['item'] full_word,\n" +
                "    row_time\n" +
                "from page_log\n" +
                "where page['item'] is not null \n" +
                "and page['item_type'] ='keyword'\n" +
//                "and page['last_page_id'] = 'search'" +
                "");
        tableEnv.createTemporaryView("search_table", searchTable);

        //TODO 5 使用自定义函数对关键字拆词
        Table splitTable = tableEnv.sqlQuery("select \n" +
                "    keyword,\n" +
                "    row_time\n" +
                "from search_table ,\n" +
                "lateral table (ik_analyze(full_word)) as t(keyword)");

        tableEnv.createTemporaryView("split_table", splitTable);

//        tableEnv.executeSql("select * from split_table").print();

        //TODO 6 分组开窗合并计算
        Table keywordBeanTable = tableEnv.sqlQuery("select \n" +
                "    date_format(TUMBLE_START(\n" +
                "    row_time, INTERVAL '10' second),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    date_format(TUMBLE_END(\n" +
                "    row_time, INTERVAL '10' second),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "\n" + "'" + EduConstant.KEYWORD_SEARCH + "' source," +
                "    0 keywordLength,\n" +
                "    keyword,\n" +
                "    count(*) keyword_count,\n" +
                "    UNIX_TIMESTAMP()*1000 ts\n" +
                "from split_table\n" +
                "group by TUMBLE(row_time, INTERVAL '10' second),keyword");

        //TODO 7 转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toDataStream(keywordBeanTable, KeywordBean.class);
        keywordBeanDataStream.print();

        //TODO 8 写出到clickHouse中
        keywordBeanDataStream.addSink(ClickHouseUtil.<KeywordBean>getJdbcSink("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        //TODO 9 运行任务
        env.execute();
    }
}
