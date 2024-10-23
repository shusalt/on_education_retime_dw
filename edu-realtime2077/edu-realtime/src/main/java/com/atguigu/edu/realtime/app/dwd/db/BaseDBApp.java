package com.atguigu.edu.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DwdBroadcastProcessFunction;
import com.atguigu.edu.realtime.bean.DimTableProcess;
import com.atguigu.edu.realtime.bean.DwdTableProcess;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author yhm
 * @create 2023-04-24 18:05
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);

        //TODO 2 读取业务topic_db主流数据
        String groupId = "base_DB_app";
        DataStreamSource<String> dbStream = env.fromSource(KafkaUtil.getKafkaConsumer("topic_db", groupId), WatermarkStrategy.noWatermarks(), "base_db");

        //TODO 3 清洗转换topic_db数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = dbStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (!("bootstrap-start".equals(type) || "bootstrap-insert".equals(type) || "bootstrap-complete".equals(type))) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        jsonObjStream.print();

        //TODO 4 使用flinkCDC读取dwd配置表数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("edu_config")
                .tableList("edu_config.dwd_table_process")
                // 定义读取数据的格式
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 设置读取数据的模式
                .startupOptions(StartupOptions.initial())
                .build();

        //TODO 5 创建广播流
        DataStreamSource<String> tableProcessStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "dwd_table_process");
        MapStateDescriptor<String, DwdTableProcess> dwdTableProcessState = new MapStateDescriptor<>("dwd_table_process_state", String.class, DwdTableProcess.class);
        BroadcastStream<String> broadcastDS = tableProcessStream.broadcast(dwdTableProcessState);

        //TODO 6 连接两个流
        BroadcastConnectedStream<JSONObject, String> connectStream = jsonObjStream.connect(broadcastDS);


        //TODO 7 过滤出需要的dwd表格数据
        SingleOutputStreamOperator<JSONObject> processStream = connectStream.process(new DwdBroadcastProcessFunction(dwdTableProcessState));

        //TODO 8 将数据写出到kafka
        processStream.sinkTo(KafkaUtil.getKafkaProducerBySchema(new KafkaRecordSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, KafkaSinkContext context, Long timestamp) {
                String topic = element.getString("sink_table");
                element.remove("sink_table");
                return new ProducerRecord<byte[], byte[]>(topic,element.toJSONString().getBytes());
            }
        },"base_db_app_trans"));

        //TODO 9 执行任务
        env.execute();
    }
}
