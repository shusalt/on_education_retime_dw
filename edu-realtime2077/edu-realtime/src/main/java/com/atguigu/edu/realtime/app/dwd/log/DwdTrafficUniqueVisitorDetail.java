package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2023-04-21 16:24
 */
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境设置状态后端
        StreamExecutionEnvironment env = EnvUtil.getExecutionEnvironment(4);

        // TODO 2 读取kafka日志主题数据
        String topicName = "dwd_traffic_page_log";
        DataStreamSource<String> pageLogStream = env.fromSource(KafkaUtil.getKafkaConsumer(topicName, "dwd_traffic_unique_visitor_detail"), WatermarkStrategy.noWatermarks(), "unique_visitor_source");

        // TODO 3 转换结构 过滤last_page_id不为空的数据
        SingleOutputStreamOperator<JSONObject> firstPageStream = pageLogStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String lastPageID = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageID == null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // TODO 4 安装mid分组
        KeyedStream<JSONObject, String> keyedStream = firstPageStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        // TODO 5 判断独立访客
        SingleOutputStreamOperator<JSONObject> filteredStream = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            ValueState<String> lastVisitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last_visit_dt", String.class);
                // 设置状态的存活时间
                stringValueStateDescriptor.enableTimeToLive(StateTtlConfig
                        .newBuilder(Time.days(1L))
                        // 设置状态的更新模式为创建及写入
                        // 每次重新写入的时候记录时间  到1天删除状态
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());

                lastVisitDtState = getRuntimeContext().getState(stringValueStateDescriptor);


            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String visitDt = DateFormatUtil.toDate(jsonObject.getLong("ts"));
                String lastVisitDt = lastVisitDtState.value();
                // 对于迟到的数据  last日期会大于visit日期  数据也不要
                if (lastVisitDt == null || (DateFormatUtil.toTs(lastVisitDt) < DateFormatUtil.toTs(visitDt))) {
                    lastVisitDtState.update(visitDt);
                    return true;
                }
                return false;
            }
        });

        // TODO 6 将独立访客数据写出到对应的kafka主题
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        SingleOutputStreamOperator<String> sinkStream = filteredStream.map((MapFunction<JSONObject, String>) JSONAware::toJSONString);
        sinkStream.sinkTo(KafkaUtil.getKafkaProducer(targetTopic,"unique_visitor_trans"));


        // TODO 7 运行任务
        env.execute();
    }
}
