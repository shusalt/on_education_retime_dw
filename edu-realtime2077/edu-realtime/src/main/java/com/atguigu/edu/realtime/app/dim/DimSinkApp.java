package com.atguigu.edu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimBroadcastProcessFunction;
import com.atguigu.edu.realtime.app.func.DimPhoenixSinkFunc;
import com.atguigu.edu.realtime.bean.DimTableProcess;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author yhm
 * @create 2023-04-19 17:06
 */
public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建flink运行环境以及设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(1);


        // TODO 2 读取主流kafka数据
        DataStreamSource<String> eduDS = env.fromSource(KafkaUtil.getKafkaConsumer("topic_db", "dim_sink_app"), WatermarkStrategy.noWatermarks(),
                "kafka_source");

        // TODO 3 对主流数据进行ETL
//        eduDS.map(new MapFunction<String, JSONObject>() {
//            @Override
//            public JSONObject map(String value) throws Exception {
//                return JSON.parseObject(value);
//            }
//        }).filter(new FilterFunction<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject jsonObject) throws Exception {
//                String type = jsonObject.getString("type");
//                if (type.equals("bootstrap-complete") || type.equals("bootstrap-start")){
//                    return false;
//                }
//                return true;
//            }
//        });

        SingleOutputStreamOperator<JSONObject> jsonDS = eduDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (!(type.equals("bootstrap-complete") || type.equals("bootstrap-start"))) {
                        // 需要的数据
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("数据转换json错误");
                }
            }
        });
//        jsonDS.print();


        // TODO 4 使用flinkCDC读取配置表数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("edu_config")
                .tableList("edu_config.table_process")
                // 定义读取数据的格式
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 设置读取数据的模式
                .startupOptions(StartupOptions.initial())
                .build();


        DataStreamSource<String> configDS = env.fromSource(mySqlSource,WatermarkStrategy.noWatermarks(),"mysql_source");

        configDS.print();
        // TODO 5 将配置表数据创建为广播流
        // key-> 维度表名称  value-> mysql单行数据 使用javaBean
        MapStateDescriptor<String, DimTableProcess> tableProcessState = new MapStateDescriptor<>("table_process_state", String.class, DimTableProcess.class);

        BroadcastStream<String> broadcastStream = configDS.broadcast(tableProcessState);

        // TODO 6 合并主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectCS = jsonDS.connect(broadcastStream);

        // TODO 7 对合并流进行分别处理
        SingleOutputStreamOperator<JSONObject> dimDS = connectCS.process(new DimBroadcastProcessFunction(tableProcessState));

        // TODO 8 调取维度数据写出到phoenix
        dimDS.addSink(new DimPhoenixSinkFunc());

        // TODO 9 执行flink任务
        env.execute();
    }
}
